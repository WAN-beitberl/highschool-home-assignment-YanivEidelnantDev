import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

import static java.lang.Integer.parseInt;
import static java.lang.Double.parseDouble;
import static java.lang.Boolean.parseBoolean;

public class Main {
    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/database1";
        String username="root";
        String password="E11235813e";

        // Run if DB isn't initialized
        //InitializeHighschool(jdbcURL, username, password);
        //InitializeFriends(jdbcURL, username, password);

        // View Student ID and their Average Grade
        ViewAvgGrade(jdbcURL, username, password);
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
                if(car_color == "") {
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

    public static void ViewAvgGrade(String jdbcURL, String username, String password) {
            try {
                Connection connection = DriverManager.getConnection(jdbcURL, username, password);
                connection.setAutoCommit(false);

                String sql = "select identification_card, grade_avg from studentsummary ";

                Statement statement = connection.createStatement();

                ResultSet resultSet = statement.executeQuery(sql);

                System.out.println("ID Card     Avg Grade ");
                while(resultSet.next())
                {
                    System.out.println(resultSet.getString(1) + "   " + resultSet.getString(2));
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
    }
}