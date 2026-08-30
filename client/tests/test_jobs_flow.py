"""Pruebas del motor de flujos, con dobles que imitan el protocolo de pasos de Inngest.

`step.run` ejecuta el manejador y **lanza** la interrupción con el resultado; el runtime la atrapa
afuera, memoiza y vuelve a invocar la función. Un doble que solo devuelva el valor no sirve para
esto: el defecto que se está cubriendo es justamente que el trabajo se ejecutaba y se botaba.
"""

from __future__ import annotations

import pytest

from job_service_sdk.jobs import JobFlow, JobRuntime, MapNode, ParallelNode, get_registered_jobs, job_flow


class FakeInterrupt(BaseException):
    """Como las de Inngest: heredan de BaseException, no de Exception."""

    def __init__(self, responses):
        super().__init__("interrupt")
        self.responses = list(responses)


class FakeStep:
    def __init__(self):
        self.memo: dict[str, object] = {}
        self.executed: list[str] = []

    async def run(self, step_id, handler, *args):
        if step_id in self.memo:
            return self.memo[step_id]
        self.executed.append(step_id)
        output = await handler(*args)
        raise FakeInterrupt([(step_id, output)])


class FakeGroup:
    def __init__(self):
        self.batches: list[int] = []

    async def parallel(self, callables):
        self.batches.append(len(callables))
        outputs, responses = [], []
        for call in callables:
            try:
                outputs.append(await call())
            except FakeInterrupt as interrupt:
                responses.extend(interrupt.responses)
        if responses:
            raise FakeInterrupt(responses)
        return tuple(outputs)


class FakeCtx:
    def __init__(self, group=None):
        self.group = group


class FakeClient:
    def __init__(self):
        self.checkpoints = 0

    def checkpoint(self, *args, **kwargs):
        self.checkpoints += 1
        return {}


def make_runtime(step, ctx):
    from uuid import uuid4

    return JobRuntime(ctx=ctx, step=step, client=FakeClient(), job_id=uuid4(),
                      job_key="prueba", state={})


async def drive(node, runtime, step, limit=50):
    """Vuelve a invocar el nodo memoizando lo reportado, como hace Inngest entre invocaciones."""
    for _ in range(limit):
        try:
            return await node.execute(runtime, progress_index=0, progress_total=1)
        except FakeInterrupt as interrupt:
            for step_id, output in interrupt.responses:
                step.memo[step_id] = output
    raise AssertionError("el nodo no terminó")


@pytest.mark.asyncio
async def test_map_runs_each_handler_once():
    """El defecto: con asyncio.gather el manejador corría y su resultado se botaba."""
    step, group = FakeStep(), FakeGroup()
    runtime = make_runtime(step, FakeCtx(group))
    runtime.state["items"] = [["a"], ["b"], ["c"], ["d"], ["e"]]
    llamadas: list[str] = []

    async def handler(item, _state):
        llamadas.append(item[0])
        return {"visto": item[0]}

    node = MapNode(node_id="lote", items_key="items", handler=handler,
                   output_key="salida", concurrency=2)
    await drive(node, runtime, step)

    assert llamadas == ["a", "b", "c", "d", "e"], "cada item se procesa una sola vez"
    assert len(step.executed) == len(set(step.executed)), "ningún paso se ejecuta dos veces"
    assert runtime.state["salida"] == [{"visto": letra} for letra in "abcde"]


@pytest.mark.asyncio
async def test_map_batches_by_concurrency():
    step, group = FakeStep(), FakeGroup()
    runtime = make_runtime(step, FakeCtx(group))
    runtime.state["items"] = [1, 2, 3, 4, 5]

    async def handler(item, _state):
        return item

    node = MapNode(node_id="lote", items_key="items", handler=handler,
                   output_key="salida", concurrency=2)
    await drive(node, runtime, step)

    assert group.batches[-3:] == [2, 2, 1], (
        "la pasada que completa el nodo reparte los 5 items en tandas de 2, 2 y 1")


@pytest.mark.asyncio
async def test_map_without_group_falls_back_to_sequential():
    """Sin `ctx.group` -detrás de un runtime viejo- el nodo sigue funcionando."""
    step = FakeStep()
    runtime = make_runtime(step, FakeCtx(group=None))
    runtime.state["items"] = [1, 2]

    async def handler(item, _state):
        return item

    node = MapNode(node_id="lote", items_key="items", handler=handler, output_key="salida")
    await drive(node, runtime, step)

    assert runtime.state["salida"] == [1, 2]


@pytest.mark.asyncio
async def test_map_with_no_items_does_not_break():
    step, group = FakeStep(), FakeGroup()
    runtime = make_runtime(step, FakeCtx(group))
    runtime.state["items"] = []

    async def handler(item, _state):
        raise AssertionError("no debería llamarse")

    node = MapNode(node_id="lote", items_key="items", handler=handler, output_key="salida")
    await drive(node, runtime, step)

    assert runtime.state["salida"] == []


@pytest.mark.asyncio
async def test_parallel_does_not_swallow_the_interrupt():
    """Antes el gather con return_exceptions se comía la interrupción y el paso no memoizaba."""
    step, group = FakeStep(), FakeGroup()
    runtime = make_runtime(step, FakeCtx(group))

    async def uno(_state):
        return {"uno": True}

    async def dos(_state):
        return {"dos": True}

    node = ParallelNode(
        node_id="ramas",
        branches=(JobFlow().step("uno", uno), JobFlow().step("dos", dos)),
    )
    await drive(node, runtime, step)

    assert runtime.state["uno"] is True
    assert runtime.state["dos"] is True
    assert len(step.executed) == len(set(step.executed))


def test_cron_registers_a_second_function():
    @job_flow(job_key="prueba_cron", display_name="Prueba", description="x",
              cron="TZ=America/Bogota 0 6 * * *", cron_input={"confirm": True})
    def _flow() -> JobFlow:
        return JobFlow()

    spec = next(s for s in get_registered_jobs() if s.job_key == "prueba_cron")
    assert spec.cron == "TZ=America/Bogota 0 6 * * *"
    assert spec.cron_input == {"confirm": True}
    assert spec.resolved_cron_fn_id == "prueba-cron-cron"
