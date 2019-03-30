from unittest.mock import patch as mock_patch


def patch_time():
    return Patcher()


class Patcher:

    def __init__(self):

        self.monotonic_time = 0.0

        # list of functions that should be patched
        self.patchers = [
            mock_patch('time.monotonic', side_effect=self.monotonic),
            mock_patch('time.sleep', side_effect=self.sleep),
        ]

    def __enter__(self):
        print("=== ENTERING MOCKED ENVIRONMENT ===")
        for patch in self.patchers:
            patch.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        print("=== LEAVING MOCKED ENVIRONMENT ===")

        for patcher in self.patchers:
            patcher.stop()

    def monotonic(self):
        return self.monotonic_time

    def sleep(self, duration):
        self.monotonic_time += duration
