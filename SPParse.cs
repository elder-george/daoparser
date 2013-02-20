using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.IO;
using System.Data;
using System.Data.SQLite;

public class User{
    public int UserId{get;set;}
    public string Name{get;set;}
    public Post[] Posts{get;set;}
}

public class Post{
    public int PostId{get;set;}
    public int UserId{get;set;}
    public string Title{get;set;}
    public string Body{get;set;}
}

public class ParserSettings<T>{
    public ParserSettings<T> IncludeList<U>(
        Expression<Func<T, IEnumerable<U>>> prop, int resultSetIdx){
            // TODO
        return this;
    }
}


public class Parser<TRoot>{

    public Parser(Action<ParserSettings<TRoot>> cfg){
        var ps = new ParserSettings<TRoot>();
        cfg(ps);
    }

    public IEnumerable<TRoot> Parse(IDataReader rdr){
        yield break; // todo;
    }
}

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

    static void ReadData(string dbName){
        using(var conn = new SQLiteConnection(String.Format("Data Source={0};version=3", dbName))){
            conn.Open();
            using(var rdr = conn.ExecuteReader(@"
SELECT UserId, Name FROM User;
SELECT PostId, UserId, title, body FROM Post;
                    ")){
                var parser = new Parser<User>(_ => _.IncludeList(u => u.Posts, 1));
                var usersWithPosts = parser.Parse(rdr);
//                new DataContractJsonSerializer(typeof(User)).WriteObject(u
            }
        }
    }

    static void Main(){
        try{
            PopulateDb(DbFileName);
            ReadData(DbFileName);

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