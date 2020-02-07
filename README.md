# Notificacións en Postgre
PostgreSQL permite que o propio xestor de base de datos avise ao cliente de que aconteceu un evento. Esto realizase cas **notificacións**.

Tedes máis información en (https://www.postgresql.org/docs/12/sql-notify.html)[https://www.postgresql.org/docs/12/sql-notify.html].

En JAVA non se lle da quitado todo o partido porque non é unha linguaxe asíncrona. Polo tanto en lugar de que o SXBD nos avise dunha notificación, teremos que cada certo tempo comprobar se hai novas notificacións.

## Exemplo
Vamos a facer un pequeno chat no que só recibimos mensaxes. Para iso teremos unha táboa que garde as mensaxes co nome de usuario correspondente.

```java
String sqlTableCreation = new String(
    "CREATE TABLE IF NOT EXISTS mensaxes ("
            + "id SERIAL PRIMARY KEY, "
            + "usuario TEXT NOT NULL, "
            + "mensaxe TEXT NOT NULL);");
CallableStatement createTable = conn.prepareCall(sqlTableCreation);
createTable.execute();
createTable.close();
```
Imos crear unha función para un trigger que xere unha notificación para cada nova mensaxe. O **channel** que usaremos será neste caso 'novamensaxe' e o **payload** o id desa mensaxe.

```java
String sqlCreateFunction = new String(
    "CREATE OR REPLACE FUNCTION notificar_mensaxe() "+
    "RETURNS trigger AS $$ "+
    "BEGIN " +
        "PERFORM pg_notify('novamensaxe',NEW.id::text); "+
    "RETURN NEW; "+
    "END; "+
    "$$ LANGUAGE plpgsql; ");
CallableStatement createFunction = conn.prepareCall(sqlCreateFunction);
createFunction.execute();
createFunction.close();
```

Creamos o trigger para que cada vez que se engada unha nova mensaxe se execute a función anterior.
```java
String sqlCreateTrigger = new String(
    "DROP TRIGGER IF EXISTS not_nova_mensaxe ON mensaxes; "+
    "CREATE TRIGGER not_nova_mensaxe "+
    "AFTER INSERT "+
    "ON mensaxes "+
    "FOR EACH ROW "+
    "EXECUTE PROCEDURE notificar_mensaxe(); ");
CallableStatement createTrigger = conn.prepareCall(sqlCreateTrigger);
createTrigger.execute();
createTrigger.close();
```
Agora subscribimonos ao canal **novamensaxe** para poder recibir notificacións.
```java
PGConnection pgconn = conn.unwrap(PGConnection.class);
Statement stmt = conn.createStatement();
stmt.execute("LISTEN novamensaxe");
stmt.close();
System.out.println("Esperando mensaxes...");
```

Cada certo tempo comprobamos se hai nova notificacións. Se hai novas notificacións realizamos unha consulta a base de datos para coller toda a informacións desa mensaxe. Utilizamos o id que se envía no payload.

```java
PGNotification notifications[] = pgconn.getNotifications();

if(notifications != null){
    for(int i=0;i < notifications.length;i++){

        int id = Integer.parseInt(notifications[i].getParameter());

        sqlMensaxe.setInt(1, id);
        ResultSet rs = sqlMensaxe.executeQuery();
        rs.next();
        System.out.println(rs.getString(1) + ":" + rs.getString(2));
        rs.close();
    }
}
```
