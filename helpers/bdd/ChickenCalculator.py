class ChickenCalculator:

    def __init__(self, insects_per_minute):
        self.insects_per_minute: int = int(insects_per_minute)

    def search_insects(self, minutes):
        return self.insects_per_minute * minutes