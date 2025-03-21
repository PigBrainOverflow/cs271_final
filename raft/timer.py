import asyncio


class Timer:
    _time: callable
    _callback: callable
    _loop: asyncio.AbstractEventLoop
    _is_active: bool
    _handle: asyncio.TimerHandle

    def __init__(self, time: callable, callback: callable, loop: asyncio.AbstractEventLoop | None = None):
        self._time = time
        self._callback = callback
        self._loop = asyncio.get_event_loop() if loop is None else loop
        self._is_active = False
        self._handler = None


    def start(self):
        self._is_active = True
        self._handler = self._loop.call_later(self._time(), self.run)


    def run(self):
        if self._is_active:
            self._callback()
            self._handler = self._loop.call_later(self._time(), self.run)


    def stop(self):
        self._is_active = False
        if self._handler:
            self._handler.cancel()


    def reset(self):
        self.stop()
        self.start()