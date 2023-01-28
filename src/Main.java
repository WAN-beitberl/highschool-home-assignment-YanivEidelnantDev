import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.module.ModuleFinder;
import java.sql.*;
import java.util.Scanner;

import static java.lang.Boolean.*;
import static java.lang.Integer.parseInt;
import static java.lang.Double.parseDouble;

public class Main {
    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/database1";
        String username="root";
        String password="E11235813e";

        // Run if DB isn't initialized
        InitializeHighschool(jdbcURL, username, password);
        InitializeFriends(jdbcURL, username, password);

        Scanner myScanner = new Scanner(System.in);
        String sql = null;
        int Input = 0;
        int ID_Card_Input = 0;
        System.out.println("Welcome to the Student Database!");
        // Main while loop
        while(true)
        {
            System.out.println("\n\nPress 1 to show the average grade of the school");
            System.out.println("Press 2 to show the average grade of boys");
            System.out.println("Press 3 to show the average grade of girls");
            System.out.println("Press 4 to show the average height of boys above 2m which have purple cars");
            System.out.println("Press 5 to show a student's first and second friend circle");
            System.out.println("Press 6 to show the percentage of popular and lonely students");
            System.out.println("Press 7 to show a specific student's average grade");
            System.out.println("Press 8 to exit the program");
            System.out.print("> ");
            Input = myScanner.nextInt();

            switch (Input){
                case 1:
                    sql = "select AVG(grade_avg) from studentsummary";
                    System.out.print("The average grade of the school is = ");
                    GeneralSql(jdbcURL, username, password, sql);
                    break;
                case 2:
                    sql = "select AVG(grade_avg) from studentsummary where gender = 'Male'";
                    System.out.print("The average grade of boys is = ");
                    GeneralSql(jdbcURL, username, password, sql);
                    break;
                case 3:
                    sql = "select AVG(grade_avg) from studentsummary where gender = 'Female'";
                    System.out.print("The average grade of girls is = ");
                    GeneralSql(jdbcURL, username, password, sql);
                    break;
                case 4:
                    sql = "select AVG(cm_heigth) from studentsummary where gender = 'Male' AND cm_heigth >= 200 AND has_car = TRUE AND car_color = 'Purple'";
                    System.out.print("The average height of boys above 2m which have purple cars is = ");
                    GeneralSql(jdbcURL, username, password, sql);
                    System.out.print("cm");
                    break;
                case 5:
                    System.out.println("Please enter student ID Card");
                    ID_Card_Input = myScanner.nextInt();
                    System.out.print("The friend circle is =");
                    Input5(jdbcURL, username, password, ID_Card_Input, TRUE);
                    break;
                case 6:
                    sql = "SELECT COUNT(DISTINCT id) / (SELECT COUNT(*) FROM studentfriendships) * 100 AS popular_percentage\n" +
                            "FROM studentfriendships\n" +
                            "WHERE friend_id IS NOT NULL AND other_friend_id IS NOT NULL;";
                    System.out.print("Percentage of popular people is = ");
                    GeneralSql(jdbcURL, username, password, sql);
                    System.out.print("%");
                    sql = "SELECT COUNT(DISTINCT id) / (SELECT COUNT(*) FROM studentfriendships) * 100 AS lonely_percentage\n" +
                            "FROM studentfriendships\n" +
                            "WHERE friend_id IS NULL AND other_friend_id IS NULL;\n";
                    System.out.print("\nPercentage of lonely people is = ");
                    GeneralSql(jdbcURL, username, password, sql);
                    System.out.print("%");
                    break;
                case 7:
                    System.out.println("Please enter student ID Card");
                    ID_Card_Input = myScanner.nextInt();
                    sql = "select grade_avg from studentsummary where identification_card = " + ID_Card_Input;
                    System.out.print("The average grade is = ");
                    GeneralSql(jdbcURL, username, password, sql);
                    break;
                case 8:
                    System.out.println("Thank you for using the Student Database!");
                    return;
                default:
                    System.out.println("Illegal Input!");
            }
        }
    }

    public static void InitializeHighschool(String jdbcURL, String username, String password)
    {
        String filePath="C:\\Users\\yaniv\\Downloads\\highschool_sql_assignment\\highschool_sql_assignment\\highschool.csv";

        int batchSize = 20;
        try {
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO studentsummary(id,first_name, last_name, email, gender, ip_address, cm_heigth, age, has_car, car_color, grade, grade_avg, identification_card) values(?,?,?,?,?,?,?,?,?,?,?,?,?)";

            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(filePath));

            String linetext=null;
            int count=0;
            lineReader.readLine(); // Skip header (arguments)

            while ((linetext=lineReader.readLine())!=null) {
                String[] data = linetext.split(",");
                String id = data[0];
                String first_name = data[1];
                String last_name = data[2];
                String email = data[3];
                String gender = data[4];
                String ip_address = data[5];
                String cm_height = data[6];
                String age = data[7];
                String has_car = data[8];
                String car_color = data[9];
                String grade = data[10];
                String grade_avg = data[11];
                String identification_card = data[12];

                statement.setInt(1, parseInt(id));
                statement.setString(2, first_name);
                statement.setString(3, last_name);
                statement.setString(4, email);
                statement.setString(5, gender);
                statement.setString(6, ip_address);
                statement.setInt(7, parseInt(cm_height));
                statement.setInt(8, parseInt(age));
                statement.setBoolean(9, parseBoolean(has_car));
                if(car_color.equals("") && has_car.equals("true")) {
                    statement.setString(10, "Unknown");
                }
                else if(has_car.equals("false")) {
                    statement.setNull(10, Types.NULL);
                }
                else {
                    statement.setString(10, car_color);
                }
                statement.setInt(11, parseInt(grade));
                statement.setDouble(12, parseDouble(grade_avg));
                statement.setInt(13, parseInt(identification_card));
                statement.addBatch();
            }
            if(count%batchSize==0)
            {
                statement.executeBatch();
            }
            lineReader.close();
            statement.executeBatch();
            connection.commit();
            connection.close();
            System.out.println("Successfully inserted to studentSummary!");
        }
        catch (Exception e)
        {
            System.out.println("studentSummary already initialized!");
        }
    }

    public static void InitializeFriends(String jdbcURL, String username, String password) {
        String filePath = "C:\\Users\\yaniv\\Downloads\\highschool_sql_assignment\\highschool_sql_assignment\\highschool_friendships2.csv";

        int batchSize = 20;
        try {
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO studentfriendships(id,friend_id, other_friend_id) values(?,?,?);";

            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(filePath));

            String linetext = null;
            int count = 0;
            lineReader.readLine(); // Skip header (arguments)

            while ((linetext = lineReader.readLine()) != null) {
                String[] data = linetext.split(",", -1);
                String id = data[0];
                String friend_id = data[1];
                String other_friend_id = data[2];

                statement.setInt(1, parseInt(id));
                if(friend_id == "") {
                    statement.setNull(2, Types.NULL);
                }
                else {
                    statement.setInt(2, parseInt(friend_id));
                }
                if(other_friend_id == "") {
                    statement.setNull(3, Types.NULL);
                }
                else {
                    statement.setInt(3, parseInt(other_friend_id));
                }
                statement.addBatch();
            }
            if (count % batchSize == 0)
            {
                statement.executeBatch();
            }
            lineReader.close();
            statement.executeBatch();
            connection.commit();
            connection.close();
            System.out.println("Successfully inserted to studentFrienships!");
        } catch (Exception e) {
            System.out.println("studentFriendships already initialized!");

        }
    }

    public static void GeneralSql(String jdbcURL, String username, String password, String sql) {
            try {
                Connection connection = DriverManager.getConnection(jdbcURL, username, password);
                connection.setAutoCommit(false);

                Statement statement = connection.createStatement();

                ResultSet resultSet = statement.executeQuery(sql);

                while(resultSet.next())
                {
                    System.out.format("%.5s",resultSet.getString(1));
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
    }

    public static void Input5(String jdbcURL, String username, String password, int ID_Card_Input, boolean Modifier) {
        try {
            Connection connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            Statement statement = connection.createStatement();

            String sql = "select friend_id, other_friend_id from studentfriendships where id = " + ID_Card_Input;

            ResultSet resultSet = statement.executeQuery(sql);
            int friend_id = 0;
            int other_friend_id = 0;
            if(resultSet.next())
            {
                friend_id = resultSet.getInt(1);
                other_friend_id = resultSet.getInt(2);
            }

            if(friend_id != 0)
            {
                System.out.print(" " + friend_id);
            }
            if(other_friend_id != 0)
            {
                System.out.print(" " + other_friend_id);
            }

            //Run one more time for the second friend circle
            if(Modifier == TRUE)
            {
                Input5(jdbcURL, username, password, friend_id, FALSE);
                Input5(jdbcURL, username, password, other_friend_id, FALSE);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}