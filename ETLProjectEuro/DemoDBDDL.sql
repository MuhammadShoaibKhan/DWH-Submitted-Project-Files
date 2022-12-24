CREATE DATABASE ETLDemo
GO

USE ETLDemo

DROP TABLE IF EXISTS ExpensesJPY

CREATE TABLE ExpensesJPY
(
	[date] datetime,
	JPY money,
	rate DECIMAL(6,5),
	CAD money
)