myinteger = 212345
number_string = str(myinteger)

number_string[-3:]


def addTwoDigits(n):
    # note that n is an integer
    # we need a workable number to start with
    num = 0    
    # we need a loop to iterate through the digits of n, but
    # we can not iterate through an int
    # so we convert it to string
    for i in str(n): 
        # we can not add strings together, so we convert back to int
        # num += x notation is like this: 
        # num = num + x 
        num += int(i)
    # that's the solution!
    return num
