package ua.kiev.prog.case2;

import ua.kiev.prog.shared.Client;
import ua.kiev.prog.shared.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;

public class Main {
    static final String DB_CONNECTION =
            "jdbc:mysql://localhost:3306/clientsdb?serverTimezone=Europe/Kiev";
    static final String DB_USER = "root";
    static final String DB_PASSWORD = "root";

    public static void main(String[] args) throws SQLException {
        ConnectionFactory factory = new ConnectionFactory(DB_CONNECTION, DB_USER, DB_PASSWORD);
        try (Scanner sc = new Scanner(System.in);
             Connection conn = factory.getConnection()){
            ClientDAOEx dao = new ClientDAOEx(conn, "clientsdb.".concat(Client.class.getSimpleName()+"s"));
            dao.init(Client.class);
            Client c = new Client("test", 1);
            dao.add(c);
            List<Client> list = dao.getAll(Client.class);
            for (Client cli : list)
                System.out.println(cli);
            List<Client> list2 = dao.getAllEx(Client.class, "name", "age");
            for (Client cli : list2)
                System.out.println(cli);
            dao.delete(list.get(0));
        }
    }
}
