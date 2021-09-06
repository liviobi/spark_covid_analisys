# Analysis of COVID-19 Data

## Description
In this project, you have to implement a program that analyzes open datasets to study the evolution of the
COVID-19 situation worldwide. The program starts from the dataset of new reported cases for each country
daily and computes the following queries:
1. Seven days moving average of new reported cases, for each county and for each day
2. Percentage increase (with respect to the day before) of the seven days moving average, for each country
and for each day
3. Top 10 countries with the highest percentage increase of the seven days moving average, for each day
You can either use real open datasets
1 or synthetic data generated with the simulator developed for Project #4.
A performance analysis of the proposed solution is appreciated (but not mandatory). In particular, we are
interested in studies that evaluate (1) how the execution time changes when increasing the size of the dataset
and/or number of countries; (2) how the execution time decreases when adding more processing cores/hosts.
## Assumptions and Guidelines
1. When using a real dataset, for countries that provide weekly reports, you can assume that the weekly
increment is evenly spread across the day of the week.

