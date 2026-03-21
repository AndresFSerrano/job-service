from collections.abc import Awaitable, Callable, Mapping

from uuid import UUID

JobHandler = Callable[[UUID, Mapping[str, object] | None], Awaitable[Mapping[str, object]]]


class JobHandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}

    def register(self, job_key: str, handler: JobHandler) -> None:
        self._handlers[job_key] = handler

    def get_handler(self, job_key: str) -> JobHandler | None:
        return self._handlers.get(job_key)


handler_registry = JobHandlerRegistry()


def handler(job_key: str) -> Callable[[JobHandler], JobHandler]:
    def decorator(func: JobHandler) -> JobHandler:
        handler_registry.register(job_key, func)
        return func

    return decorator
