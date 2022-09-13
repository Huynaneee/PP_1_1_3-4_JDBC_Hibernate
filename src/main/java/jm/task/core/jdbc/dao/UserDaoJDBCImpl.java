package jm.task.core.jdbc.dao;


import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        Connection connection = null;
        try {
            connection = Util.getInstance().getConnection();
            Statement statement = ((java.sql.Connection) connection).createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS users (\n" +
                    "id BIGINT PRIMARY KEY NOT NULL AUTO_INCREMENT,\n" +
                    "name VARCHAR(45) NOT NULL,\n" +
                    "lastNAME VARCHAR(45) NOT NULL,\n" +
                    "age TINYINT NOT NULL\n" + ");");
            try {
                statement.close();
            } catch (SQLException e) {
                connection.rollback();
                System.out.println("Ошибка при закрытии создания таблицы");
            }
            connection.commit();
        } catch (SQLException e){
            System.out.println("Не получилось создать таблицу");
        } finally {
            try {
                if (connection != null){
                    connection.close();
                }
            } catch (Exception e){
                System.out.println("Не получилось закрыть соединение");
            }
        }
    }



    public void dropUsersTable() {
        Connection connection = null;
        try {
            connection = Util.getInstance().getConnection();
            Statement statement = connection.createStatement();
            statement.execute("DROP TABLE IF EXISTS users");
            try {
                statement.close();
            } catch (SQLException e){
                connection.rollback();
                System.out.println("Не получилось добавить в таблицу");
            } connection.commit();
        } catch (SQLException e){
            System.out.println("Ошибка добавления");
        } finally {
            try {
                if (connection != null){
                    connection.close();
                }
            } catch (Exception e){
                System.out.println("Ошибка закрытия соединения");
            }
        }

    }

    public void saveUser(String name, String lastName, byte age) {
        Connection connection = null;
        try {
            connection = Util.getInstance().getConnection();
            PreparedStatement prepareStatement = connection.prepareStatement("INSERT INTO users ( name, lastName, age) VALUES(?,?,?)");
            prepareStatement.setString(1, name);
            prepareStatement.setString(2, lastName);
            prepareStatement.setByte(3, age);
            prepareStatement.executeUpdate();

            try {
                prepareStatement.close();
            } catch (SQLException e) {
                connection.rollback();
                System.out.println("Не удалось закрыть соединения сохранения User");
            } connection.commit();
            System.out.println("User с именем - " + name + " добавлен в базу данных");


        } catch (SQLException e) {
            System.out.println("Не удалось сохранить пользователя");
        } finally {
            try {
                if (connection != null){
                    connection.close();
                }
            } catch (Exception e){
                System.out.println("Не удалось закрыть соединение");
            }
        }

    }

    public void removeUserById(long id) {
        Connection connection = null;

        try {
            connection = Util.getInstance().getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM users where id = ?");
            preparedStatement.setInt(1,(int) id);
            preparedStatement.executeUpdate();
            try {
                preparedStatement.close();
            } catch (SQLException e){
                connection.rollback();
                System.out.println("Ошибка при удалении пользователя");
            } connection.commit();

        } catch (SQLException e){
            System.out.println("Не удалось удалить пользователя");
        } finally {
            try {
                if (connection != null){
                    connection.close();
                }
            } catch (Exception e){
                System.out.println("Ошибка закрытия соединения");
            }
        }

    }

    public List<User> getAllUsers() {
        Connection connection = null;
        List<User> users = new ArrayList<>();

        try {

            connection = Util.getInstance().getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM users");

            while (resultSet.next()) {
                User user = new User(resultSet.getString("name"),
                        resultSet.getString("lastName"),
                        resultSet.getByte("age"));

                user.setId(resultSet.getLong("id"));
                users.add(user);
            }

            try {
                statement.close();
                resultSet.close();
            } catch (SQLException e){
                connection.rollback();
                System.out.println("Ошибка при закрытии добавления пользователя");
            }
            connection.commit();

        } catch (SQLException e) {
            System.out.println("Ошибка при добавлении пользователя");

        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                System.out.println("Ошибка закрытия соединения");
            }
        }
        return users;
    }

    public void cleanUsersTable() {
        Connection connection = null;

        try {
            connection = Util.getInstance().getConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate("TRUNCATE TABLE  users");
            try {
                statement.close();
            } catch (SQLException e){
                connection.rollback();
                System.out.println("Ошибка при очистке таблицы");
            } connection.commit();
        } catch (SQLException e ){
            System.out.println("Не удалось очистить таблицу");
        } finally {
            try {
                if (connection != null){
                    connection.close();
                }
            } catch (Exception e){
                System.out.println("Ошибка закрытия соединения!");
            }
        }
    }
}
