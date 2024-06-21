@ApacheSpark
Feature: Employee and department operations

  Scenario: Calculating average salary per dept when employee data is present
    Given there is 'employee' dataframe with following data:
      |employee_id |  employee_name | dept_id | salary |
      |101 |  "Rohit P"     | 10 | 1000   |
      |102 |  "Pooja P"     | 10 | 1000   |
      |103 |  "Rutu M"      | 10 | 400    |
      |104 |  "Rushi M"     | 20 | 4000   |
      |105 |  "Prithvi D"   | 20 | 6000   |
      |106 |  "Rajani D"    | 30 | 10000  |
      |107 |  "Shrikant D"  | 30 | 5000   |
      |108 |  "Rahul S"     | 30 | 3000   |
    When calculating average salary per department
    Then result is 'avg_sal_per_dept' dataframe with following lines:
      |dept_id  | avg_sal_per_dept |
      |10       | 800.0            |
      |20       | 5000.0           |
      |30       | 6000.0           |