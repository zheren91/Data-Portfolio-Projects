# Use atguigudb 
USE atguigudb

# ------------------------------------------------Select and basic functions

# Select the employee's last name, their employee id, and their annual salary
SELECT last_name,employee_id,salary * 12 * (1 + IFNULL(commission_pct,0)) "Annual Salary"
FROM employees;

# Select the employees whos last name contains the letter A or K
SELECT last_name
FROM employees
WHERE last_name LIKE '%a%k%' OR last_name LIKE '%k%a%';

# Select the employee whos first name end with the letter E
# or the third letter of their last name is the letter A
SELECT employee_id,first_name, last_name
FROM employees
WHERE first_name LIKE '%e' OR last_name LIKE '__a%';

# Select the employee whos department ID between 20 to 50, and their last name
SELECT  department_id,last_name
FROM employees
WHERE department_id IN(20, 50);

# Concatenate last name, their earnings, and their ideal salary as their current
# salary times three, remove the zeros and call it their ideal salary
SELECT CONCAT(last_name, ' earns ', TRUNCATE(salary, 0) , ' monthly but wants ',
TRUNCATE(salary * 3, 0)) "Ideal Salary"
FROM employees;

# Use Case-when to create salary grades based on salary
SELECT MAX(salary),MIN(salary),AVG(salary)
FROM employees;
# Create salary grades, select employee_id, last_name
SELECT employee_id, last_name, 
CASE        
	WHEN salary BETWEEN 2000 AND 4000 THEN 'A'
	WHEN salary BETWEEN 4001 AND 6000 THEN 'B'
	WHEN salary BETWEEN 6001 AND 8000 THEN 'C'
	WHEN salary BETWEEN 8001 AND 10000 THEN 'D'
	WHEN salary BETWEEN 10001 AND 12000 THEN 'E'
	WHEN salary BETWEEN 12001 AND 14000 THEN 'F'
	WHEN salary BETWEEN 14001 AND 16000 THEN 'G'
	WHEN salary BETWEEN 16001 AND 18000 THEN 'H'
	WHEN salary BETWEEN 18001 AND 20000 THEN 'I'
	ELSE 'J' END 'Salary Grade'
FROM employees;	    


# Select the employee id, last name, department id, and email address,
# their email address should contain the letter e, and the query needs
# to be ordered by the length of email on descending order, and department id
# as ascending order
SELECT employee_id, last_name, department_id, email
FROM employees
WHERE email IS NOT NULL AND email REGEXP '[e]'
ORDER BY LENGTH(email) DESC, department_id ASC;

# ------------------------------------------------ Working with Dates

# Select the years the employee has been working, and the number of days they've worked since they were hired
SELECT DATEDIFF(CURDATE(),hire_date)/365 "year worked", DATEDIFF(CURDATE(),hire_date) "days worked"
FROM employees;

# Select the last name, hire date, department id for those who have been hired after 1997
# and their department id is either 80, 90 or 110, and must have commissions
SELECT last_name, hire_date, department_id
FROM employees
# where hire_date >= '1997-01-01'
# where hire_date >= str_to_date('1997-01-01', '%Y-%m-%d')
WHERE DATE_FORMAT(hire_date,'%Y') >= '1997'
AND department_id IN(80,90,110)
AND commission_pct IS NOT NULL;


# Select the employees who have worked for more than 1000 days
SELECT last_name, hire_date
FROM employees
WHERE DATEDIFF(CURDATE(),hire_date) > 1000;


# ------------------------------------------------ Joining tables

# Select the last name, job id, department id from employees whos location is in Toronto
SELECT last_name , e.department_id ,job_id , department_name
FROM employees e, departments d, locations l
WHERE e.department_id = d.department_id
AND d.location_id = l.location_id
AND city = 'Toronto';

# Select the department without any employees
SELECT e.last_name, d.department_id
FROM departments d LEFT JOIN employees e 
ON e.department_id = d.department_id
WHERE last_name IS NULL;


# Self join and determine employees and their manager's name and employee number
SELECT emp.last_name "employees", emp.employee_id "employee number", 
mgr.last_name "manager name", mgr.employee_id "manager number"
FROM employees emp LEFT JOIN employees mgr
ON emp.manager_id = mgr.employee_id;

# Select the last name, commission percentage, department name location id and city, only include those who
# have commission
SELECT e.last_name, e.commission_pct, d.department_name, d.location_id, l.city
FROM employees e LEFT JOIN departments d ON e.department_id = d.department_id
LEFT JOIN locations l ON l.location_id = d.location_id
WHERE commission_pct IS NOT NULL;

# Select all information for department ID which is less than 90 and their email contails the letter A
SELECT * FROM employees WHERE email LIKE '%a%'
UNION
SELECT * FROM employees WHERE department_id > 90;

# Select the last name, job id, salary, department name and street address for those
# who's role is executive or sales
SELECT e.last_name, e.job_id, e.salary, d.department_name, l.street_address
FROM employees e LEFT JOIN departments d
ON e.department_id = d.department_id
LEFT JOIN locations l ON l.location_id = d.location_id
WHERE department_name IN('Executive','Sales');


# ------------------------------------------------ Subqueries

# Query the employee id and last names of employees in the same department
# as the employee whose name contains the letter 'u'
SELECT last_name, employee_id
FROM employees
WHERE department_id = (SELECT department_id, last_name
			FROM employees
			WHERE last_name LIKE '%u%');

# Query the employee id and last name of the employee whose location_id is 1700
SELECT e.employee_id, e.last_name
FROM employees e LEFT JOIN departments d
ON e.department_id = d.department_id
WHERE d.department_id IN (SELECT d.department_id
				FROM departments
				WHERE d.location_id = 1700)

# Query the manager whos last name is King and their salary
SELECT emp.last_name, emp.salary
FROM employees emp JOIN employees mgr
ON emp.manager_id = mgr.employee_id 
WHERE mgr.last_name = (SELECT last_name
			FROM employees
			WHERE last_name = 'King' AND manager_id IS NOT NULL);

# Select the all information of the department with the lowest average salary
SELECT d.*, (SELECT AVG(salary) FROM employees WHERE department_id = d.department_id) avg_sal
FROM departments d
WHERE department_id = (
			SELECT department_id
			FROM employees
			GROUP BY department_id
			HAVING AVG(salary) <=
						ALL(SELECT AVG(salary)
						FROM employees
						GROUP BY department_id));

# Select the all information for the job with the highest average salary
SELECT * 
FROM jobs
WHERE job_id = (
			SELECT job_id
			FROM employees
			GROUP BY job_id
			HAVING AVG(salary) >= ALL(
						SELECT AVG(salary)
						FROM employees
						GROUP BY job_id));

# Query the department which their average is salary is higher than overall average salary
SELECT department_id, AVG(salary)
FROM employees 
WHERE department_id IS NOT NULL
GROUP BY department_id
HAVING AVG(salary) > (SELECT AVG(salary)
			FROM employees);
			
# Query the manager information with the highest salary department on last_name, department_id, email, salary
SELECT employee_id, last_name, department_id, email, salary
FROM employees
WHERE employee_id IN(
			SELECT manager_id
			FROM employees
			WHERE department_id = (SELECT department_id
						FROM employees
						GROUP BY department_id
						HAVING AVG(salary) >= ALL(SELECT AVG(salary)
									FROM employees
									GROUP BY department_id)));

# Query the employee id, last name, hire date, salary, and their manager's last name is 'De Haan'
SELECT emp.employee_id, emp.last_name, emp.hire_date, emp.salary
FROM employees emp JOIN employees mgr
ON emp.manager_id = mgr.employee_id
WHERE emp.manager_id IN (SELECT manager_id
			FROM employees mgr
			WHERE manager_id IN(SELECT mgr.employee_id
						FROM employees mgr
						WHERE last_name = 'De Haan') );
						

# Query the department id and department name with more than 4 workers						
SELECT department_name, department_id
FROM departments 
WHERE department_id IN(SELECT e.department_id
			FROM employees e JOIN departments d
			ON e.department_id = d.department_id
			GROUP BY department_id
			HAVING COUNT(employee_id) > 4);



