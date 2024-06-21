from behave import *

from helpers.bdd.ChickenCalculator import ChickenCalculator

chicken_calculator: ChickenCalculator = ChickenCalculator(0)
result: int = 0


@given(u'a chicken collects {num_of_insects} insects per minute')
def step_impl(context, num_of_insects):
    global chicken_calculator
    print(context)
    chicken_calculator = ChickenCalculator(int(num_of_insects))


@when(u'chicken has searched insects for {num_of_minutes} minutes')
def step_impl(context, num_of_minutes):
    global chicken_calculator, result
    print(context)
    result = chicken_calculator.search_insects(int(num_of_minutes))


@when(u'chicken has searched insects for {hrs} hours')
def step_impl(context, hrs):
    print(context)
    global chicken_calculator, result
    num_of_minutes = int(hrs) * 60
    result = chicken_calculator.search_insects(int(num_of_minutes))


@then(u'the chicken has found {expected_total_num_of_insects} insects')
def step_impl(context, expected_total_num_of_insects):
    global result
    print(context)
    expected_result = int(expected_total_num_of_insects)
    assert result == expected_result, f"Expected result {expected_result} is not matched with actual result {result}!!"


# to test feature file - run the command in terminal
# behave features/ChickenCalculator.feature