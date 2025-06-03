class Vehicle:
    def __init__(self, max_speed=100):
        # Initialize the underlying attribute.
        # IMPORTANT: Call the property setter here if you want its logic to run during init.
        self.max_speed = max_speed # This will call Vehicle's own setter
        print(f"Vehicle __init__: _max_speed (after setter) is {self._max_speed}")

    @property
    def max_speed(self):
        print("Vehicle getter for max_speed called")
        return self._max_speed

    @max_speed.setter
    def max_speed(self, value):
        print(f"Vehicle setter for max_speed called with value: {value}")
        if not isinstance(value, (int, float)):
            raise TypeError("Max speed must be a number.")
        if value < 0:
            raise ValueError("Max speed cannot be negative.")
        if value > 300: # A general vehicle limit
            print("Vehicle setter: Max speed too high, capping at 300.")
            self._max_speed = 300
        else:
            self._max_speed = value
        print(f"Vehicle setter: _max_speed is now {self._max_speed}")

    def display_info(self):
        print(f"Vehicle Info: Max Speed = {self.max_speed}")


class Car(Vehicle):
    def __init__(self, max_speed=120, model="Generic"):
        # Call super().__init__ AFTER Car's specific attributes are ready,
        # if the superclass __init__ relies on overridden methods/properties from the subclass.
        # Or, set them after super().__init__().
        # In this case, Vehicle.__init__ calls self.max_speed = max_speed,
        # which will correctly call Car's setter if Car is instantiated.
        self.model = model # Set model first if Car's setter logic depends on it
        super().__init__(max_speed) # This will trigger Car's setter, then Vehicle's
        print(f"Car __init__: model set to {self.model}, _max_speed is {self._max_speed}")

    @property
    def max_speed(self):
        print("Car getter for max_speed called")
        # To get the value through the superclass's getter (and its logic):
        return super().max_speed
        # Or, if you know the superclass stores it in _max_speed and you want to bypass its getter:
        # return self._max_speed

    @max_speed.setter
    def max_speed(self, value):
        print(f"Car setter for max_speed called with value: {value}")
        if not isinstance(value, (int, float)): # Car specific check
            raise TypeError("Car max speed must be a numeric value.")

        # Car-specific logic
        if self.model == "SportsCar" and value > 250:
            print("Car setter: SportsCar attempting high speed. Allowing through Car setter.")
            # Call the superclass's setter using the property assignment syntax
            super(Car, type(self)).max_speed.__set__(self, value) # Explicit call to super's setter
            # OR the more common Pythonic way:
            # super().max_speed = value
        elif 'model' in self.__dict__ and self.model != "SportsCar" and value > 200: # Regular car limit
            print("Car setter: Max speed for a regular car is too high, capping at 200.")
            super(Car, type(self)).max_speed.__set__(self, 200)
            # super().max_speed = 200
        else:
            # Delegate to superclass setter for other cases/default logic
            super(Car, type(self)).max_speed.__set__(self, value)
            # super().max_speed = value
        print(f"Car setter: _max_speed (after super call) is now {self._max_speed}")

    def display_info(self):
        super().display_info()
        print(f"Car Model: {self.model}")

# --- Test Cases ---
print("--- Vehicle Tests ---")
v = Vehicle(150)
print(f"Initial v.max_speed (direct access to _max_speed): {v._max_speed}")
v.max_speed = 50
print(f"After v.max_speed = 50 (direct access to _max_speed): {v._max_speed}")
v.max_speed = 350
print(f"After v.max_speed = 350 (direct access to _max_speed): {v._max_speed}")
try:
    v.max_speed = -10
except ValueError as e:
    print(f"Error: {e}")
v.display_info()

print("\n--- Car Tests ---")
my_car = Car(max_speed=180, model="Sedan") # Car init -> Vehicle init -> Car setter -> Vehicle setter
print(f"Initial my_car._max_speed: {my_car._max_speed}")

my_car.max_speed = 190 # Calls Car setter, then Vehicle setter
print(f"After my_car.max_speed = 190: {my_car._max_speed}")

my_car.max_speed = 220 # Calls Car setter (caps at 200 for Sedan), then Vehicle setter
print(f"After my_car.max_speed = 220: {my_car._max_speed}")

my_car.max_speed = 80
print(f"After my_car.max_speed = 80: {my_car._max_speed}")
my_car.display_info()

print("\n--- SportsCar Test ---")
sports_car = Car(max_speed=260, model="SportsCar")
print(f"Initial sports_car._max_speed: {sports_car._max_speed}")

sports_car.max_speed = 280 # Car setter allows, Vehicle setter applies its logic
print(f"After sports_car.max_speed = 280: {sports_car._max_speed}")

sports_car.max_speed = 320 # Car setter allows, Vehicle setter caps at 300
print(f"After sports_car.max_speed = 320: {sports_car._max_speed}")

try:
    sports_car.max_speed = -5
except ValueError as e:
    print(f"Error for sports_car: {e}")
sports_car.display_info()

try:
    invalid_car = Car(max_speed="fast", model="Test")
except TypeError as e:
    print(f"Error during Car init: {e}")

v_invalid = Vehicle(max_speed=100) # This will now also raise TypeError in Vehicle setter
