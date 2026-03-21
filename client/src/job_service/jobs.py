from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
import logging
import time
from typing import TYPE_CHECKING, Any
from uuid import UUID

from job_service.client import JobDefinitionConfig, JobServiceClient, JobServiceError

if TYPE_CHECKING:
    import inngest

logger = logging.getLogger(__name__)

FlowState = dict[str, Any]
FlowPredicate = Callable[[FlowState], bool]
StepHandler = Callable[[FlowState], Awaitable[dict[str, Any] | None]]
MapItemHandler = Callable[[Any, FlowState], Awaitable[dict[str, Any] | None]]


def _state_keys(state: FlowState) -> list[str]:
    return sorted(str(key) for key in state.keys())


async def _call_job_service_with_retry(
    func: Callable[..., Any],
    *args: Any,
    attempts: int = 4,
    delays: tuple[float, ...] = (0.15, 0.35, 0.75),
    **kwargs: Any,
) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        except JobServiceError as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            await asyncio.sleep(delays[min(attempt - 1, len(delays) - 1)])
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("retry helper reached unexpected state")


@dataclass(slots=True)
class JobRuntime:
    ctx: Any
    step: Any
    client: JobServiceClient
    job_id: UUID
    job_key: str
    state: FlowState
    _event_buffer: list[dict[str, Any]] = field(default_factory=list)
    _pending_progress: dict[str, Any] | None = field(default=None)
    _scope_prefix: str = field(default="")
    _is_branch: bool = field(default=False)

    def _scoped_id(self, step_id: str) -> str:
        return f"{self._scope_prefix}:{step_id}" if self._scope_prefix else step_id

    async def add_event(
        self,
        event_type: str,
        message: str,
        *,
        level: str = "info",
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._event_buffer.append({
            "event_type": event_type,
            "message": message,
            "level": level,
            "data": data,
        })
        return {}

    async def add_node_event(
        self,
        event_type: str,
        message: str,
        *,
        node_id: str,
        node_type: str,
        level: str = "info",
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event_data = {
            "job_key": self.job_key,
            "node_id": node_id,
            "node_type": node_type,
            "state_keys": _state_keys(self.state),
        }
        if data:
            event_data.update(data)
        return await self.add_event(
            event_type,
            message,
            level=level,
            data=event_data,
        )

    async def report_progress(
        self,
        *,
        progress_current: int,
        progress_total: int,
        progress_label: str,
    ) -> dict[str, Any]:
        if not self._is_branch:
            self._pending_progress = {
                "progress_current": progress_current,
                "progress_total": progress_total,
                "progress_label": progress_label,
            }
        return {}

    async def flush(self) -> None:
        if self._is_branch:
            return
        events = list(self._event_buffer)
        progress = self._pending_progress
        if not events and not progress:
            return
        self._event_buffer.clear()
        self._pending_progress = None
        try:
            await _call_job_service_with_retry(
                self.client.checkpoint,
                self.job_id,
                events=events,
                **(progress or {}),
            )
        except JobServiceError:
            logger.exception(
                "No se pudo persistir checkpoint para job %s",
                self.job_id,
            )

    async def run_step(
        self,
        step_id: str,
        handler: Callable[..., Awaitable[Any]],
        *args: Any,
        node_type: str = "step",
        extra_event_data: dict[str, Any] | None = None,
    ) -> Any:
        event_data = {"node_id": step_id, "node_type": node_type}
        if extra_event_data:
            event_data.update(extra_event_data)
        await self.add_node_event(
            "step_started",
            f"Step iniciado: {step_id}",
            node_id=step_id,
            node_type=node_type,
            data=extra_event_data,
        )
        started = time.perf_counter()
        try:
            result = await self.step.run(self._scoped_id(step_id), handler, *args)
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            await self.add_node_event(
                "step_failed",
                f"Step falló: {step_id}",
                node_id=step_id,
                node_type=node_type,
                level="error",
                data={**event_data, "duration_ms": duration_ms, "error": str(exc)},
            )
            await self.flush()
            raise

        duration_ms = int((time.perf_counter() - started) * 1000)
        event_payload = {**event_data, "duration_ms": duration_ms}
        if isinstance(result, dict):
            event_payload["result_keys"] = sorted(str(key) for key in result.keys())
        await self.add_node_event(
            "step_completed",
            f"Step completado: {step_id}",
            node_id=step_id,
            node_type=node_type,
            data=event_payload,
        )
        await self.flush()
        return result


class JobFlowError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class FlowNode:
    node_id: str

    async def execute(
        self,
        runtime: JobRuntime,
        *,
        progress_index: int,
        progress_total: int,
    ) -> int:
        raise NotImplementedError

    def count_nodes(self) -> int:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class DelayNode(FlowNode):
    duration: timedelta

    async def execute(
        self,
        runtime: JobRuntime,
        *,
        progress_index: int,
        progress_total: int,
    ) -> int:
        await runtime.report_progress(
            progress_current=progress_index,
            progress_total=progress_total,
            progress_label=self.node_id,
        )
        await runtime.add_node_event(
            "delay_started",
            f"Delay iniciado: {self.node_id}",
            node_id=self.node_id,
            node_type="delay",
            data={"seconds": int(self.duration.total_seconds())},
        )
        started = time.perf_counter()
        await runtime.step.sleep(self.node_id, self.duration)
        duration_ms = int((time.perf_counter() - started) * 1000)
        await runtime.add_node_event(
            "delay_completed",
            f"Delay completado: {self.node_id}",
            node_id=self.node_id,
            node_type="delay",
            data={"seconds": int(self.duration.total_seconds()), "duration_ms": duration_ms},
        )
        await runtime.flush()
        return progress_index + 1

    def count_nodes(self) -> int:
        return 1


@dataclass(frozen=True, slots=True)
class StepNode(FlowNode):
    handler: StepHandler

    async def execute(
        self,
        runtime: JobRuntime,
        *,
        progress_index: int,
        progress_total: int,
    ) -> int:
        await runtime.report_progress(
            progress_current=progress_index,
            progress_total=progress_total,
            progress_label=self.node_id,
        )
        result = await runtime.run_step(
            self.node_id,
            self.handler,
            runtime.state,
            node_type="step",
        )
        if isinstance(result, dict):
            runtime.state.update(result)
        return progress_index + 1

    def count_nodes(self) -> int:
        return 1


@dataclass(frozen=True, slots=True)
class ChoiceNode(FlowNode):
    predicate: FlowPredicate
    then_nodes: tuple[FlowNode, ...]
    else_nodes: tuple[FlowNode, ...] = ()

    async def execute(
        self,
        runtime: JobRuntime,
        *,
        progress_index: int,
        progress_total: int,
    ) -> int:
        await runtime.report_progress(
            progress_current=progress_index,
            progress_total=progress_total,
            progress_label=self.node_id,
        )

        async def _evaluate_choice(state: FlowState, predicate: FlowPredicate) -> dict[str, Any]:
            branch_name = "then" if predicate(state) else "else"
            return {"branch_name": branch_name}

        choice_result = await runtime.run_step(
            f"{self.node_id}:evaluate",
            _evaluate_choice,
            runtime.state,
            self.predicate,
            node_type="choice",
        )
        branch_name = str(choice_result["branch_name"])
        branch = self.then_nodes if branch_name == "then" else self.else_nodes
        branch_name = "then" if branch is self.then_nodes else "else"
        await runtime.add_node_event(
            "choice_evaluated",
            f"Choice evaluado: {self.node_id}",
            node_id=self.node_id,
            node_type="choice",
            data={"branch": branch_name},
        )
        current = progress_index + 1
        await runtime.add_node_event(
            "choice_branch_started",
            f"Choice rama iniciada: {self.node_id}",
            node_id=self.node_id,
            node_type="choice",
            data={"branch": branch_name, "branch_nodes": len(branch)},
        )
        for node in branch:
            current = await node.execute(
                runtime,
                progress_index=current,
                progress_total=progress_total,
            )
        await runtime.add_node_event(
            "choice_branch_completed",
            f"Choice rama completada: {self.node_id}",
            node_id=self.node_id,
            node_type="choice",
            data={"branch": branch_name, "branch_nodes": len(branch)},
        )
        return current

    def count_nodes(self) -> int:
        return 1 + max(
            sum(node.count_nodes() for node in self.then_nodes),
            sum(node.count_nodes() for node in self.else_nodes),
        )


@dataclass(frozen=True, slots=True)
class FailNode(FlowNode):
    message: str

    async def execute(
        self,
        runtime: JobRuntime,
        *,
        progress_index: int,
        progress_total: int,
    ) -> int:
        await runtime.add_node_event(
            "flow_failed",
            f"Flow falló: {self.node_id}",
            node_id=self.node_id,
            node_type="fail",
            level="error",
            data={"message": self.message},
        )
        raise JobFlowError(self.message)

    def count_nodes(self) -> int:
        return 1


@dataclass(frozen=True, slots=True)
class LoopUntilNode(FlowNode):
    condition: FlowPredicate
    body_nodes: tuple[FlowNode, ...]
    max_iterations: int
    delay: timedelta | None = None

    async def execute(
        self,
        runtime: JobRuntime,
        *,
        progress_index: int,
        progress_total: int,
    ) -> int:
        current = progress_index
        for iteration in range(1, self.max_iterations + 1):
            async def _evaluate_loop_condition(state: FlowState, predicate: FlowPredicate) -> dict[str, Any]:
                return {"condition_met": predicate(state)}

            condition_result = await runtime.run_step(
                f"{self.node_id}:condition:{iteration}",
                _evaluate_loop_condition,
                runtime.state,
                self.condition,
                node_type="loop-condition",
                extra_event_data={"iteration": iteration},
            )
            if condition_result["condition_met"]:
                await runtime.add_node_event(
                    "loop_condition_met",
                    f"Loop completado: {self.node_id}",
                    node_id=self.node_id,
                    node_type="loop",
                    data={"iterations": iteration - 1, "iteration": iteration, "phase": "pre"},
                )
                return current

            await runtime.add_node_event(
                "loop_iteration_started",
                f"Loop iteración {iteration}: {self.node_id}",
                node_id=self.node_id,
                node_type="loop",
                data={"iteration": iteration, "max_iterations": self.max_iterations},
            )
            for node in self.body_nodes:
                current = await node.execute(
                    runtime,
                    progress_index=current,
                    progress_total=progress_total,
                )
            condition_result = await runtime.run_step(
                f"{self.node_id}:condition-post:{iteration}",
                _evaluate_loop_condition,
                runtime.state,
                self.condition,
                node_type="loop-condition",
                extra_event_data={"iteration": iteration},
            )
            if condition_result["condition_met"]:
                await runtime.add_node_event(
                    "loop_condition_met",
                    f"Loop completado: {self.node_id}",
                    node_id=self.node_id,
                    node_type="loop",
                    data={"iterations": iteration, "iteration": iteration, "phase": "post"},
                )
                return current

            await runtime.add_node_event(
                "loop_iteration_completed",
                f"Loop iteración completada: {self.node_id}",
                node_id=self.node_id,
                node_type="loop",
                data={"iteration": iteration, "max_iterations": self.max_iterations},
            )

            if self.delay is not None and iteration < self.max_iterations:
                await runtime.report_progress(
                    progress_current=current,
                    progress_total=progress_total,
                    progress_label=f"{self.node_id}:delay",
                )
                started = time.perf_counter()
                await runtime.add_node_event(
                    "loop_delay_started",
                    f"Loop delay iniciado: {self.node_id}",
                    node_id=self.node_id,
                    node_type="loop",
                    data={"iteration": iteration, "seconds": int(self.delay.total_seconds())},
                )
                await runtime.step.sleep(f"{self.node_id}:delay:{iteration}", self.delay)
                duration_ms = int((time.perf_counter() - started) * 1000)
                await runtime.add_node_event(
                    "loop_delay_completed",
                    f"Loop delay completado: {self.node_id}",
                    node_id=self.node_id,
                    node_type="loop",
                    data={
                        "iteration": iteration,
                        "seconds": int(self.delay.total_seconds()),
                        "duration_ms": duration_ms,
                    },
                )

        await runtime.add_node_event(
            "loop_exhausted",
            f"Loop agotado: {self.node_id}",
            node_id=self.node_id,
            node_type="loop",
            level="error",
            data={"max_iterations": self.max_iterations},
        )
        raise JobFlowError(f"Loop '{self.node_id}' agotó {self.max_iterations} iteraciones")

    def count_nodes(self) -> int:
        body_count = sum(node.count_nodes() for node in self.body_nodes)
        if body_count == 0:
            body_count = 1
        pre_condition_count = self.max_iterations
        post_condition_count = self.max_iterations
        delay_count = max(self.max_iterations - 1, 0) if self.delay is not None else 0
        return (body_count * self.max_iterations) + pre_condition_count + post_condition_count + delay_count


@dataclass(frozen=True, slots=True)
class ParallelNode(FlowNode):
    branches: tuple[JobFlow, ...]

    async def execute(
        self,
        runtime: JobRuntime,
        *,
        progress_index: int,
        progress_total: int,
    ) -> int:
        await runtime.report_progress(
            progress_current=progress_index,
            progress_total=progress_total,
            progress_label=self.node_id,
        )
        await runtime.add_node_event(
            "parallel_started",
            f"Paralelo iniciado: {self.node_id}",
            node_id=self.node_id,
            node_type="parallel",
            data={"branches": len(self.branches)},
        )

        branch_runtimes: list[JobRuntime] = []

        async def run_branch(branch_index: int, branch: JobFlow) -> FlowState:
            branch_runtime = JobRuntime(
                ctx=runtime.ctx,
                step=runtime.step,
                client=runtime.client,
                job_id=runtime.job_id,
                job_key=runtime.job_key,
                state=dict(runtime.state),
                _scope_prefix=f"{self.node_id}:branch-{branch_index}",
                _is_branch=True,
            )
            branch_runtimes.append(branch_runtime)
            for node in branch.nodes:
                await node.execute(
                    branch_runtime,
                    progress_index=0,
                    progress_total=branch.total_progress_nodes() or 1,
                )
            return branch_runtime.state

        results = await asyncio.gather(
            *[run_branch(i, branch) for i, branch in enumerate(self.branches)],
            return_exceptions=True,
        )

        # Recolectar eventos de todas las ramas en el buffer del padre
        for br in branch_runtimes:
            runtime._event_buffer.extend(br._event_buffer)

        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            await runtime.add_node_event(
                "parallel_failed",
                f"Paralelo falló: {self.node_id}",
                node_id=self.node_id,
                node_type="parallel",
                level="error",
                data={"error": str(errors[0]), "failed_branches": len(errors)},
            )
            await runtime.flush()
            raise errors[0]

        for result in results:
            if isinstance(result, dict):
                runtime.state.update(result)

        await runtime.add_node_event(
            "parallel_completed",
            f"Paralelo completado: {self.node_id}",
            node_id=self.node_id,
            node_type="parallel",
            data={"branches": len(self.branches)},
        )
        await runtime.flush()
        return progress_index + 1

    def count_nodes(self) -> int:
        return 1


@dataclass(frozen=True, slots=True)
class MapNode(FlowNode):
    items_key: str
    handler: MapItemHandler
    output_key: str | None = None
    concurrency: int = 0  # 0 = todas en paralelo

    async def execute(
        self,
        runtime: JobRuntime,
        *,
        progress_index: int,
        progress_total: int,
    ) -> int:
        items: list[Any] = runtime.state.get(self.items_key, [])
        await runtime.report_progress(
            progress_current=progress_index,
            progress_total=progress_total,
            progress_label=self.node_id,
        )
        await runtime.add_node_event(
            "map_started",
            f"Map iniciado: {self.node_id}",
            node_id=self.node_id,
            node_type="map",
            data={"items_key": self.items_key, "items_count": len(items)},
        )

        async def process_item(index: int, item: Any) -> Any:
            return await runtime.step.run(
                runtime._scoped_id(f"{self.node_id}:{index}"),
                self.handler,
                item,
                runtime.state,
            )

        if self.concurrency > 0:
            results: list[Any] = []
            for batch_start in range(0, len(items), self.concurrency):
                batch = items[batch_start : batch_start + self.concurrency]
                batch_results = await asyncio.gather(
                    *[process_item(batch_start + i, item) for i, item in enumerate(batch)]
                )
                results.extend(batch_results)
        else:
            results = list(
                await asyncio.gather(*[process_item(i, item) for i, item in enumerate(items)])
            )

        if self.output_key:
            runtime.state[self.output_key] = results

        await runtime.add_node_event(
            "map_completed",
            f"Map completado: {self.node_id}",
            node_id=self.node_id,
            node_type="map",
            data={"items_processed": len(items), "output_key": self.output_key},
        )
        await runtime.flush()
        return progress_index + 1

    def count_nodes(self) -> int:
        return 1


@dataclass(slots=True)
class JobFlow:
    nodes: list[FlowNode] = field(default_factory=list)
    error_nodes: list[FlowNode] = field(default_factory=list)

    def delay(
        self,
        node_id: str,
        *,
        minutes: int = 0,
        seconds: int = 0,
    ) -> JobFlow:
        self.nodes.append(
            DelayNode(
                node_id=node_id,
                duration=timedelta(minutes=minutes, seconds=seconds),
            )
        )
        return self

    def step(self, node_id: str, handler: StepHandler) -> JobFlow:
        self.nodes.append(StepNode(node_id=node_id, handler=handler))
        return self

    def choice(
        self,
        node_id: str,
        *,
        predicate: FlowPredicate,
        then_flow: JobFlow,
        else_flow: JobFlow | None = None,
    ) -> JobFlow:
        self.nodes.append(
            ChoiceNode(
                node_id=node_id,
                predicate=predicate,
                then_nodes=tuple(then_flow.nodes),
                else_nodes=tuple(else_flow.nodes if else_flow is not None else []),
            )
        )
        return self

    def loop_until(
        self,
        node_id: str,
        *,
        condition: FlowPredicate,
        body: JobFlow,
        max_iterations: int,
        delay_seconds: int | None = None,
    ) -> JobFlow:
        self.nodes.append(
            LoopUntilNode(
                node_id=node_id,
                condition=condition,
                body_nodes=tuple(body.nodes),
                max_iterations=max_iterations,
                delay=timedelta(seconds=delay_seconds) if delay_seconds is not None else None,
            )
        )
        return self

    def parallel(self, node_id: str, *, branches: list[JobFlow]) -> JobFlow:
        self.nodes.append(ParallelNode(node_id=node_id, branches=tuple(branches)))
        return self

    def map(
        self,
        node_id: str,
        *,
        items_key: str,
        handler: MapItemHandler,
        output_key: str | None = None,
        concurrency: int = 0,
    ) -> JobFlow:
        self.nodes.append(
            MapNode(
                node_id=node_id,
                items_key=items_key,
                handler=handler,
                output_key=output_key,
                concurrency=concurrency,
            )
        )
        return self

    def fail(self, node_id: str, *, message: str) -> JobFlow:
        self.nodes.append(FailNode(node_id=node_id, message=message))
        return self

    def on_error(self, error_flow: JobFlow) -> JobFlow:
        self.error_nodes = list(error_flow.nodes)
        return self

    def total_progress_nodes(self) -> int:
        return sum(node.count_nodes() for node in self.nodes)


@dataclass(frozen=True, slots=True)
class JobFlowSpec:
    job_key: str
    display_name: str
    description: str
    flow: JobFlow
    payload_schema: dict[str, Any] | None
    result_schema: dict[str, Any] | None
    fn_id: str | None = None
    trigger_event: str | None = None
    retries: int = 0

    @property
    def resolved_fn_id(self) -> str:
        return self.fn_id or self.job_key.replace("_", "-")

    @property
    def resolved_trigger_event(self) -> str:
        return self.trigger_event or f"jobs/{self.job_key}.requested"


_registry: list[JobFlowSpec] = []


def job_flow(
    *,
    job_key: str,
    display_name: str,
    description: str,
    payload_schema: dict[str, Any] | None = None,
    result_schema: dict[str, Any] | None = None,
    fn_id: str | None = None,
    trigger_event: str | None = None,
    retries: int = 0,
) -> Callable[[Callable[[], JobFlow]], Callable[[], JobFlow]]:
    def decorator(factory: Callable[[], JobFlow]) -> Callable[[], JobFlow]:
        _registry.append(
            JobFlowSpec(
                job_key=job_key,
                display_name=display_name,
                description=description,
                flow=factory(),
                payload_schema=payload_schema,
                result_schema=result_schema,
                fn_id=fn_id,
                trigger_event=trigger_event,
                retries=retries,
            )
        )
        return factory

    return decorator


def get_registered_jobs() -> list[JobFlowSpec]:
    return list(_registry)


def get_registered_job(job_key: str) -> JobFlowSpec | None:
    for spec in _registry:
        if spec.job_key == job_key:
            return spec
    return None


def build_job_definition_configs(client_key: str) -> list[JobDefinitionConfig]:
    return [
        JobDefinitionConfig(
            client_key=client_key,
            job_key=spec.job_key,
            display_name=spec.display_name,
            description=spec.description,
            payload_schema=spec.payload_schema,
            result_schema=spec.result_schema,
            execution_engine="inngest",
            execution_ref=spec.resolved_trigger_event,
        )
        for spec in get_registered_jobs()
    ]


async def _run_flow(spec: JobFlowSpec, runtime: JobRuntime) -> dict[str, Any]:
    progress_total = max(spec.flow.total_progress_nodes(), 1)
    progress_index = 0
    try:
        for node in spec.flow.nodes:
            progress_index = await node.execute(
                runtime,
                progress_index=progress_index,
                progress_total=progress_total,
            )
        return runtime.state
    except Exception:
        if spec.flow.error_nodes:
            await runtime.add_node_event(
                "error_branch_started",
                "Ejecutando rama de error",
                node_id="error-handler",
                node_type="error-branch",
                level="error",
                data={"error_nodes": len(spec.flow.error_nodes)},
            )
            for node in spec.flow.error_nodes:
                progress_index = await node.execute(
                    runtime,
                    progress_index=progress_index,
                    progress_total=max(progress_total, progress_index + 1),
                )
            await runtime.add_node_event(
                "error_branch_completed",
                "Rama de error completada",
                node_id="error-handler",
                node_type="error-branch",
                level="error",
                data={"error_nodes": len(spec.flow.error_nodes)},
            )
        raise


def build_inngest_functions(
    *,
    client: Any,
    get_job_service_client: Callable[[], JobServiceClient],
) -> list[Any]:
    import inngest

    functions: list[Any] = []

    for spec in get_registered_jobs():
        @client.create_function(
            fn_id=spec.resolved_fn_id,
            name=spec.display_name,
            retries=spec.retries,
            trigger=inngest.TriggerEvent(event=spec.resolved_trigger_event),
        )
        async def _workflow(
            ctx: Any,
            step: Any,
            _spec: JobFlowSpec = spec,
        ) -> dict[str, Any]:
            client_instance = get_job_service_client()
            job_id = UUID(str(ctx.event.data["job_id"]))
            job_input = dict(ctx.event.data.get("job_input") or {})
            job_metadata = dict(ctx.event.data.get("job_metadata") or {})
            runtime = JobRuntime(
                ctx=ctx,
                step=step,
                client=client_instance,
                job_id=job_id,
                job_key=_spec.job_key,
                state={"job_input": job_input, "job_metadata": job_metadata},
            )
            try:
                inngest_run_id = str(ctx.run_id)
                try:
                    await _call_job_service_with_retry(
                        client_instance.update_execution,
                        job_id,
                        worker_id="inngest",
                        inngest_run_id=inngest_run_id,
                    )
                except JobServiceError:
                    logger.exception(
                        "No se pudo persistir metadata inicial del run para job %s",
                        job_id,
                    )
                await runtime.add_event(
                    "workflow_started",
                    "Workflow Inngest iniciado",
                    data={
                        "attempt": ctx.attempt,
                        "engine": "inngest",
                        "run_id": inngest_run_id,
                        "job_key": _spec.job_key,
                        "trigger_event": _spec.resolved_trigger_event,
                        "state_keys": _state_keys(runtime.state),
                    },
                )
                result = await _run_flow(_spec, runtime)
                await runtime.flush()
                await _call_job_service_with_retry(
                    client_instance.complete_execution,
                    job_id,
                    result=result,
                    result_summary=str(result.get("result_summary", "Job completado")),
                )
                await runtime.add_event(
                    "workflow_completed",
                    "Workflow Inngest completado",
                    data={"job_key": _spec.job_key, "state_keys": _state_keys(runtime.state)},
                )
                await runtime.flush()
                return result
            except Exception as exc:
                await runtime.flush()
                await _call_job_service_with_retry(
                    client_instance.fail_execution,
                    job_id,
                    error_message=str(exc),
                )
                await runtime.add_event(
                    "workflow_failed",
                    "Workflow Inngest falló",
                    level="error",
                    data={"error": str(exc), "job_key": _spec.job_key, "state_keys": _state_keys(runtime.state)},
                )
                await runtime.flush()
                raise

        functions.append(_workflow)

    return functions
