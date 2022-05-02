using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Data;
using System.Linq;
using System.Text;
using System.IO;
using DaoParser;
using System.Data.SQLite;

[DataContract]
public class User
{
    [DataMember] public int UserId { get; set; }
    [DataMember] public string Name { get; set; }
    [DataMember] public Post[] Posts { get; set; }
    [DataMember] public Avatar Avatar { get; set; }
    [DataMember] public int? SessionNumberDoNotSave { get; set; }
}
[DataContract]
public class Post
{
    [DataMember] public int PostId { get; set; }
    [DataMember] public int UserId { get; set; }
    [DataMember] public string Title { get; set; }
    [DataMember] public string Body { get; set; }
    [DataMember] public List<Comment> Comments { get; set; }
}
[DataContract]
public class Comment
{
    [DataMember] public int CommentId { get; set; }
    [DataMember] public int PostId { get; set; }
    [DataMember] public int? UserId { get; set; }
    [DataMember] public string Title { get; set; }
    [DataMember] public string Body { get; set; }
    //    [DataMember] public User[] Authors{get;set;}    // degenerated case actually
}

[DataContract]
public class Avatar
{
    //    [DataMember] public int AvatarId{get;set;}
    [DataMember] public int UserId { get; set; }
    [DataMember] public string Uri { get; set; }
    [DataMember] public int Width { get; set; }
    [DataMember] public int Height { get; set; }
}

class Program
{
    const string DbFileName = "testdb.db3";

    static void PopulateDb(string dbName)
    {
        try
        {
            File.Delete(dbName);
        }
        catch (IOException) { }
        using (var conn = new SQLiteConnection(
                String.Format("Data Source={0};version=3", dbName)))
        {
            conn.Open();
            conn.ExecuteNonQuery("CREATE TABLE User ( UserId int, Name varchar(100))");
            conn.ExecuteNonQuery("CREATE TABLE Post ( PostId int, UserId int, Title varchar(100), Body varchar(1000))");
            conn.ExecuteNonQuery("CREATE TABLE Comment ( CommentId int, PostId int, UserId int, Title varchar(100), Body varchar(1000))");
            conn.ExecuteNonQuery("CREATE TABLE Avatar ( AvatarId int, UserId int, Width int, Height int, Url varchar(1000))");

            conn.ExecuteNonQuery("INSERT INTO User Values (1, 'User1'), (2, 'User2')");

            conn.ExecuteNonQuery(@"
INSERT INTO Post Values 
(1, 1, 'Post11', 'Long text 11'),
(2, 1, 'Post12', 'Long text 12'),
(3, 2, 'Post21', 'Long text 21'),
(4, 2, 'Post22', 'Long text 22')");
            conn.ExecuteNonQuery(@"INSERT INTO Comment VALUES
(1, 1, 2, 'Comment11', 'Lorem ipsum dolor sit amet'),
(2, 2, 2, 'Comment12', 'consectetur adipiscing elit'),
(3, 3, 1, 'Comment21', 'sed do eiusmod tempor incididunt '),
(4, 4, 1, 'Comment22', 'ut labore et dolore magna aliqua'),
(5, 4, NULL, 'AnonymousComment', 'Yo mama is so fat that...')
");
            conn.ExecuteNonQuery(@"INSERT INTO Avatar VALUES
(1, 1, 100, 100, 'http://example.com/img1.png'),
(2, 2, 100, 100, 'http://example.com/img2.png')
");
        }
    }

    static void ReadData(string dbName)
    {
        using (var tm = new TimeMeasure())
        {
            var usersParser = new Parser<User>(_ =>
            {
                _.IgnoreAllMisses();
                _.IncludeList(u => u.Posts,
                              u => u.UserId,
                              p => p.UserId, 1)
                    .IncludeList(p => p.Comments, p => p.PostId, c => c.PostId, 2);
                _.IncludeSingle(u => u.Avatar,
                                u => u.UserId,
                                a => a.UserId, 3).Rename(a => a.Uri, "Url");
                //.IncludeList(c => c.Authors, c=>c.UserId, u=>u.UserId, 0)
            });
            tm.MarkEnd("Create Parser 1");
            var avatarParser = new Parser<Avatar>(_ => _.Rename(a => a.Uri, "Url"));
            tm.MarkEnd("Create Parser 2");


            using (var conn = new SQLiteConnection(String.Format("Data Source={0};version=3", dbName)))
            {
                conn.Open();
                tm.MarkEnd("Connection.Open");
                const int ReadNumber = 100;
                for (var i = 0; i < ReadNumber; i++)
                {
                    using (var rdr = conn.ExecuteReader(@"
SELECT UserId, Name FROM User;
SELECT PostId, UserId, title, body FROM Post;
SELECT CommentId, PostId, UserId, Title, Body FROM Comment;
SELECT UserId, Width, Height, Url from Avatar;
                        "))
                    {
                        tm.MarkEnd("ExecuteReader 1-" + i);
                        var usersWithPosts = usersParser.Parse(rdr).ToArray();
                        tm.MarkEnd("Parse");
                        if (i == ReadNumber - 1)
                        {
                            PrintCollectionJson(usersWithPosts);
                            tm.MarkEnd("Print Collection 1");
                        }
                    }
                }

                for (var i = 0; i < ReadNumber; i++)
                {
                    using (var rdr = conn.ExecuteReader(@"SELECT * FROM Avatar"))
                    {
                        tm.MarkEnd("Execute Reader 2-" + i);
                        var avatars = avatarParser.Parse(rdr).ToArray();
                        tm.MarkEnd("Parse");
                        if (i == ReadNumber - 1)
                        {
                            PrintCollectionJson(avatars);
                            tm.MarkEnd("Print Collection 2");
                        }
                    }
                }
            }
        }
    }

    static void PrintCollectionJson<T>(T[] entities)
    {
        using (var str = new MemoryStream())
        {
            new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof(T[])).WriteObject(str, entities);
            Console.WriteLine(Encoding.UTF8.GetString(str.ToArray()));
            Console.WriteLine();
        }
    }

    static void Main()
    {
        try
        {
            PopulateDb(DbFileName);
            ReadData(DbFileName);

        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
    }
}

class TimeMeasure : IDisposable
{
    readonly System.Diagnostics.Stopwatch _sw = System.Diagnostics.Stopwatch.StartNew();
    IList<KeyValuePair<string, long>> _marks = new List<KeyValuePair<string, long>>();
    bool _isDisposed;

    public void MarkEnd(string operationName)
    {
        _marks.Add(new KeyValuePair<string, long>(operationName, _sw.ElapsedMilliseconds));
    }

    void IDisposable.Dispose()
    {
        if (_isDisposed) throw new ObjectDisposedException("Object already disposed");
        _isDisposed = true;
        _sw.Stop();
        long prev = 0;
        for (var i = 0; i < _marks.Count; i++)
        {
            var operationLength = _marks[i].Value - prev;
            Console.WriteLine("{0}:{1}", _marks[i].Key, operationLength);
            prev = _marks[i].Value;
        }
        Console.WriteLine("Total: {0}", _sw.ElapsedMilliseconds);
    }
}

static class Extensions
{
    public static void ExecuteNonQuery(this IDbConnection conn, string stmt)
    {
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = stmt;
            cmd.ExecuteNonQuery();
        }
    }

    public static IDataReader ExecuteReader(this IDbConnection conn, string query)
    {
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = query;
            return cmd.ExecuteReader();
        }
    }
}