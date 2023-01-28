The data in the csv files had issues which included:

when read, an empty cell isn't interpreted as a null value, but rather as empty space ("") (specifically an issue in highschool_friends.csv)
"has_car" and "car_color" columns in highschool.csv not being related to each other (in other words having a car didn't mean the car had a color, and not having a car didn't mean the "car" didn't have a color)

To fix these issues, i had to catch those instances with java and insert appropriate values accordingly. for the first issue, all i had to do was check if the value was an empty space ("") and inserted the number 0 instead (first id is 1)
for the second issue, i had to check firstly if a car existed and had no color, and in that instance i would enter the string "Unknown". Otherwise, i check if the person has no car, and in that instance i inserted "None" since no value should be there if there is no car. and lastly if the person does have a car and color, i inserted them normally.
