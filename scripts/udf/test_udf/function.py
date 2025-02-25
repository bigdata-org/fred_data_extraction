import sys


def add(a:int,b:int) -> int :
    return a+b 



def subtract(a: int, b: int) -> int:
    return a - b


if __name__ == '__main__':
    if len(sys.argv) > 3:
        operation = sys.argv[1]
        a, b = map(int, sys.argv[2:4])

        if operation == "add":
            print(add(a, b))
        elif operation == "subtract":
            print(subtract(a, b))
        else:
            print("invalid")
    else:
        print("error")