package ua.kiev.prog.case2;

import ua.kiev.prog.shared.Client;

import java.sql.Connection;

public class ClientDAOEx extends AbstractDAO<Integer, Client> {
    public ClientDAOEx(Connection conn, String table) {
        super(conn, table);
    }
}
