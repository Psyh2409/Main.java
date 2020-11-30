package ua.kiev.prog.case2;

import ua.kiev.prog.shared.ColumnName;
import ua.kiev.prog.shared.Id;
import ua.kiev.prog.shared.Varchar;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDAO<K, T> {
    private final Connection conn;
    private final String table;

    public AbstractDAO(Connection conn, String table) {
        this.conn = conn;
        this.table = table;
    }

    public void add(T t) {
        try (Statement st = conn.createStatement()) {
            Field[] fields = t.getClass().getDeclaredFields();
            StringBuilder names = new StringBuilder();
            StringBuilder values = new StringBuilder();
            for (Field f : fields) {
                f.setAccessible(true);
                names.append(getNameForColumn(f)).append(',');
                values.append('"').append(f.get(t)).append("\",");
            }
            names.deleteCharAt(names.length() - 1);
            values.deleteCharAt(values.length() - 1);
            String sql = "INSERT INTO " + table + "(" + names.toString() +
                    ") VALUES(" + values.toString() + ")";
            st.execute(sql);
        } catch (SQLException | IllegalAccessException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    public void update(T t) {
        try (Statement st = conn.createStatement()) {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = null;
            for (Field f : fields) {
                if (f.isAnnotationPresent(Id.class)) {
                    id = f;
                    id.setAccessible(true);
                    break;
                }
            }
            if (id == null) throw new RuntimeException("No Id field");
            StringBuilder sb = new StringBuilder();
            for (Field f : fields) {
                if (f != id) {
                    f.setAccessible(true);
                    sb.append(getNameForColumn(f))
                            .append(" = ")
                            .append('"')
                            .append(f.get(t))
                            .append('"')
                            .append(',');
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            String sql = String.format("UPDATE %s SET %s WHERE %s = \"%s\"", table, sb.toString(), id.getName(), id.get(t));
            st.execute(sql);
        } catch (SQLException | IllegalAccessException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    public void delete(T t) {
        try (Statement st = conn.createStatement()) {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = null;
            for (Field f : fields) {
                if (f.isAnnotationPresent(Id.class)) {
                    id = f;
                    id.setAccessible(true);
                    break;
                }
            }
            if (id == null) throw new RuntimeException("No Id field");
            String sql = String.format("DELETE FROM %s WHERE %s = \"%s\"", table, getNameForColumn(id), id.get(t));
            st.execute(sql);
        } catch (SQLException | IllegalAccessException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    public List<T> getAll(Class<T> cls) {
        List<T> res = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM " + table)) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                T t = cls.newInstance();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    String columnName = md.getColumnName(i);
                    Field field = getColumnNameField(cls, columnName);
                    field.setAccessible(true);
                    field.set(t, rs.getObject(columnName));
                }
                res.add(t);
            }
            return res;
        } catch (SQLException | NoSuchFieldException | InstantiationException | IllegalAccessException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    public List<T> getAllEx(Class<T> cls, String... names) {
        List<T> res = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM " + table)) {
            while (rs.next()) {
                T t = cls.newInstance();
                for (String name : names) {
                    Field field = getColumnNameField(cls, name);
                    field.setAccessible(true);
                    field.set(t, rs.getObject(getNameForColumn(field)));
                }
                res.add(t);
            }
            return res;

        } catch (SQLException | NoSuchFieldException | InstantiationException | IllegalAccessException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    public void init(Class<T> t) {
        StringBuilder crtTbl = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        try (Statement statement = conn.createStatement()) {
            crtTbl.append("clientsdb.").append(t.getSimpleName()).append("s (");
            Class clazz = Class.forName(t.getName());
            Field[] fields = clazz.getDeclaredFields();
            for (Field f: fields) {
                if (f.isAnnotationPresent(Id.class)) {
                    crtTbl.append(getNameForColumn(f))
                            .append(" ")
                            .append(f.getType())
                            .append(" PRIMARY KEY NOT NULL AUTO_INCREMENT,");
                } else {
                    if (f.isAnnotationPresent(Varchar.class)) {
                        crtTbl.append(getNameForColumn(f))
                                .append(" VARCHAR(")
                                .append(f.getAnnotation(Varchar.class).capacity())
                                .append("),");
                    } else {
                        crtTbl.append(getNameForColumn(f))
                                .append(" ")
                                .append(f.getType())
                                .append(",");
                    }
                }
            }
            String sql = crtTbl.substring(0, crtTbl.length() - 1).concat(")");
            statement.execute(sql);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private String getNameForColumn(Field f) {
        return    f.isAnnotationPresent(ColumnName.class)
                ? f.getAnnotation(ColumnName.class).name()
                : f.getName();
    }

    private Field getColumnNameField(Class<T> t, String s) throws NoSuchFieldException {
        for (Field f : t.getDeclaredFields()) {
            if (f.isAnnotationPresent(ColumnName.class)) {
                if (f.getAnnotation(ColumnName.class).name().equals(s)) {
                    return f;
                }
            }
        }
        return t.getDeclaredField(s);
    }
}
