using System;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.IO;
using System.Data.SQLite;


[DataContract]
public class User{
    [DataMember] public int UserId{get;set;}
    [DataMember] public string Name{get;set;}
    [DataMember] public Post[] Posts{get;set;}
    [DataMember] public Avatar Avatar{get;set;}
}
[DataContract]
public class Post{
    [DataMember] public int PostId{get;set;}
    [DataMember] public int UserId{get;set;}
    [DataMember] public string Title{get;set;}
    [DataMember] public string Body{get;set;}
    [DataMember] public Comment[] Comments{get;set;}
}
[DataContract]
public class Comment{
    [DataMember] public int CommentId{get;set;}
    [DataMember] public int PostId{get;set;}
    [DataMember] public int? UserId{get;set;}
    [DataMember] public string Title{get;set;}
    [DataMember] public string Body{get;set;}
//    [DataMember] public User[] Authors{get;set;}    // degenerated case actually
}

[DataContract]
public class Avatar{
//    [DataMember] public int AvatarId{get;set;}
    [DataMember] public int UserId{get;set;}
    [DataMember] public string Uri{get;set;}
    [DataMember] public int Width{get;set;}
    [DataMember] public int Height{get;set;}
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
            conn.ExecuteNonQuery("CREATE TABLE Comment ( CommentId int, PostId int, UserId int, Title varchar(100), Body varchar(1000))");
            conn.ExecuteNonQuery("CREATE TABLE Avatar ( AvatarId int, UserId int, Width int, Height int, Uri varchar(1000))");

            conn.ExecuteNonQuery("INSERT INTO User Values (1, 'User1'), (2, 'User2')");

            conn.ExecuteNonQuery(@"
INSERT INTO Post Values 
(1, 1, 'Post11', 'Long text 11'),
(2, 1, 'Post12', 'Long text 12'),
(3, 2, 'Post21', 'Long text 21'),
(4, 2, 'Post22', 'Long text 22')");
            conn.ExecuteNonQuery(@"INSERT INTO Comment VALUES
(1, 1, 2, 'Comment11', 'OP is fag'),
(2, 2, 2, 'Comment12', 'OP is fag'),
(3, 3, 1, 'Comment21', 'OP is fag'),
(4, 4, 1, 'Comment22', 'OP is fag'),
(5, 4, NULL, 'AnonymousComment', 'OP is still fag')
");
            conn.ExecuteNonQuery(@"INSERT INTO Avatar VALUES
(1, 1, 100, 100, 'http://example.com/img1.png'),
(2, 2, 100, 100, 'http://example.com/img2.png')
");
        }
    }

    static void ReadData(string dbName){
        using(var conn = new SQLiteConnection(String.Format("Data Source={0};version=3", dbName))){
            conn.Open();
            using(var rdr = conn.ExecuteReader(@"
SELECT UserId, Name FROM User;
SELECT PostId, UserId, title, body FROM Post;
SELECT CommentId, PostId, UserId, Title, Body FROM Comment;
SELECT UserId, Width, Height, Uri from Avatar;
                    ")){
                var parser = new Parser<User>(_ => {
                                    _.IncludeList(u => u.Posts,
                                                  u => u.UserId,
                                                  p => p.UserId, 1)
                                        .IncludeList(p => p.Comments, p=>p.PostId, c=>c.PostId, 2);
                                    _.IncludeSingle(u => u.Avatar,
                                                    u => u.UserId,
                                                    a => a.UserId, 3);
                                            //.IncludeList(c => c.Authors, c=>c.UserId, u=>u.UserId, 0)
                                            });
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var usersWithPosts = parser.Parse(rdr).ToArray();
                Console.WriteLine(sw.ElapsedMilliseconds);

                PrintCollectionJson(usersWithPosts);
            }
            using(var rdr = conn.ExecuteReader(@"SELECT * FROM Avatar")){
                var avatarParser = new Parser<Avatar>( _ => {});
                PrintCollectionJson(avatarParser.Parse(rdr).ToArray());
            }
        }
    }

    static void PrintCollectionJson<T>(T[] entities){
        using (var str = new MemoryStream()){
            new DataContractJsonSerializer(typeof(T[])).WriteObject(str, entities);
           Console.WriteLine(Encoding.UTF8.GetString(str.ToArray()));
           Console.WriteLine();
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