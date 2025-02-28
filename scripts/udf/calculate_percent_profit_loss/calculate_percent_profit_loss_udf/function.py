import sys

def calculate_percent_profit_loss(value1: float, value2: float) -> float:
    
    if value1 is None or value2 is None:
        raise ValueError("One or both values are not available in the table.")
    
    if value1 == 0:
        raise ValueError("Initial value is zero, percentage change cannot be computed.")
    
    return ((value2 - value1) / value1) * 100

def main(value1: float, value2: float) -> float:
    return calculate_percent_profit_loss(float(value1), float(value2))

# For Local Debugging
if __name__ == "__main__":
    if len(sys.argv) > 2:
        print(main(*map(float, sys.argv[1:3])))
    else:
        print("Usage: script.py <value1> <value2>")