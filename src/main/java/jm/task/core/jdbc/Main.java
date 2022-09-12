package jm.task.core.jdbc;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        // реализуйте алгоритм здесь
        UserService service = new UserServiceImpl();

        service.createUsersTable();
        service.cleanUsersTable();


        service.saveUser("Anton", "Chehov", (byte) 30);
        service.saveUser("Alex", "Pushkin", (byte) 33);
        service.saveUser("Semen", "Slepakov", (byte) 42);
        service.saveUser("Lev", "Tolstoi", (byte) 74);

        List<User> users = service.getAllUsers();
        for (User user : users) {
            System.out.println(user);
        }

        service.cleanUsersTable();
        service.dropUsersTable();

    }

}
