class LamportClock:
    def __init__(self):
        self.time = 0

    def tick(self):
        """Increment the clock for a local event."""
        self.time += 1
        return self.time

    def update(self, received_timestamp):
        """Update the clock when receiving a message."""
        self.time = max(self.time, received_timestamp) + 1
        return self.time

    def get_time(self):
        """Return the current logical timestamp."""
        return self.time