using System;
using System.IO;
using System.Data;
using System.Data.SQLite;

class Program{
    const string DbFileName = "testdb.db3";

    static void PopulateDb(string dbName){
        try{
            File.Delete(dbName);
        }catch(IOException){}
        using(var conn = new SQLiteConnection(
                String.Format("Data Source={0};version=3", dbName))){
            conn.Open();
            conn.ExecuteNonQuery("CREATE TABLE User ( UserId int, Name varchar(100))");
            conn.ExecuteNonQuery("CREATE TABLE Post ( PostId int, UserId int, Title varchar(100), Body varchar(1000))");

            conn.ExecuteNonQuery("INSERT INTO User Values (1, 'User1'), (2, 'User2')");

            conn.ExecuteNonQuery(@"
INSERT INTO Post Values 
(1, 1, 'Post11', 'Long text 11'),
(2, 1, 'Post12', 'Long text 12'),
(3, 2, 'Post21', 'Long text 21'),
(4, 2, 'Post22', 'Long text 22')");
        }
    }


    static void Main(){
        try{
            PopulateDb(DbFileName);


        }catch(Exception e){
            Console.WriteLine(e);
        }
    }
}

static class Extensions{
    public static void ExecuteNonQuery(this IDbConnection conn, string stmt)
    {
        using(var cmd = conn.CreateCommand()){
            cmd.CommandText = stmt;
            cmd.ExecuteNonQuery();
        }
    }

    public static IDataReader ExecuteReader(this IDbConnection conn, string query){
        using(var cmd = conn.CreateCommand()){
            cmd.CommandText = query;
            return cmd.ExecuteReader();
        }
    }
}