using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.IO;
using System.Data;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Data.SQLite;


[DataContract]
public class User{
    [DataMember]
    public int UserId{get;set;}
    [DataMember]
    public string Name{get;set;}
    [DataMember]
    public Post[] Posts{get;set;}
}
[DataContract]
public class Post{
    [DataMember]
    public int PostId{get;set;}
    [DataMember]
    public int UserId{get;set;}
    [DataMember]
    public string Title{get;set;}
    [DataMember]
    public string Body{get;set;}
}

public interface IParserSettings{
    IEnumerable<IEntityReader> CreateReaders();
}

public class ParserSettings<T>: IParserSettings where T:new(){
    IDictionary<PropertyInfo, IIncludeResolver<T>> _resolvers = new Dictionary<PropertyInfo, IIncludeResolver<T>>();

    IList<IParserSettings> _includes = new List<IParserSettings>();
    int _resultSetIdx;

    public ParserSettings(int resultSetIdx){
        _resultSetIdx  = resultSetIdx;
    }

    public ParserSettings<T> IncludeList<U, TKey>(
            Expression<Func<T, U[]>> listProp, 
            Expression<Func<T, TKey>> foreign,
            Expression<Func<U, TKey>> primary, 
            int resultSetIdx) where U:new(){
        var member = (MemberExpression)listProp.Body;
        var prop = (PropertyInfo)member.Member;
        _resolvers[prop] = CreateResolver(listProp, foreign, primary);
        _includes.Add(new ParserSettings<U>(_resultSetIdx+1)); //TODO: if they go nested then overlaps are possible!!!
        return this;
    }

    public IEnumerable<IEntityReader> CreateReaders(){
        var readers = new List<IEntityReader>();
        var setters = new List<IPropertySetter<T>>();
        var resolvers = new List<IIncludeResolver<T>>();
        foreach(var prop in typeof(T).GetProperties()){
            if (!_resolvers.ContainsKey(prop)){
                setters.Add(CreateSetter(prop));
            } else {
                resolvers.Add(_resolvers[prop]);
            }
        }
        readers.Add(new EntityReader<T>(resolvers, setters, _resultSetIdx));
        foreach(var include in _includes){
            readers.AddRange(include.CreateReaders());
        }
        return readers;
    }

    IIncludeResolver<T> CreateResolver<U, TKey>(
            Expression<Func<T, U[]>> listProp, 
            Expression<Func<T, TKey>> foreign,
            Expression<Func<U, TKey>> primary){
        
        var param = Expression.Parameter(typeof(U[]));
        return new IncludeResolver<T, U, TKey>(
            Expression.Lambda<Action<T, U[]>>(
                Expression.Assign(listProp.Body, param), 
                new ParameterExpression[]{listProp.Parameters[0], param}).Compile(),
            foreign.Compile(),
            primary.Compile(),
            _resultSetIdx+1);
    }


    IPropertySetter<T> CreateSetter(PropertyInfo prop){
        var propType = prop.PropertyType;
        var actionType = typeof(Action<,>).MakeGenericType(typeof(T), propType);
        var set = Delegate.CreateDelegate(actionType,prop.GetSetMethod());
        var convertType = typeof(Func<,>).MakeGenericType(typeof(object), propType);
        Delegate convert;

        if (propType.IsValueType && propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>)){
            convert = Delegate.CreateDelegate(convertType, this.GetType().GetMethod("ConvertNullable", BindingFlags.Static));
        } else {
            convert = Delegate.CreateDelegate(convertType, 
                this.GetType().GetMethod("Convert", BindingFlags.Static|BindingFlags.Public)
                    .MakeGenericMethod(propType));
        }
        return (IPropertySetter<T>)Activator.CreateInstance(
                    typeof(PropertySetter<,>).MakeGenericType(typeof(T), propType),
                    new object[]{ set, convert, prop.Name}, 
                    null);
            
    }

    public static V? ConvertNullable<V>(object val) where V:struct{
        if (val == DBNull.Value) return null;
        return (V)val;
    }

    public static V Convert<V>(object val){
        if (val == DBNull.Value) return default(V);
        return (V)val;
    }

}

public interface IEntityReader{
    int ResultSetIndex{get;}
    void ReadRow(IDataReader rdr);
    void ResolveIncludes(IList<IEntityReader> resultSets);
    IEnumerable<Object> Rows{get;}
}

interface IPropertySetter<T>{
    void SetValue(T target, IDataReader reader);
}

class PropertySetter<T, U>: IPropertySetter<T>{
    Action<T, U> _set;
    Func<object, U> _convert;
    string _propertyName;

    public PropertySetter(Action<T, U> set, Func<object, U> convert, string propertyName){
        _set = set;
        _convert = convert;
        _propertyName = propertyName;
    }

    public void SetValue(T entity, IDataReader reader){
        Console.WriteLine("Reading property {0}", _propertyName);
        _set(entity, _convert(reader[_propertyName]));
    }
}

interface IIncludeResolver<T>{
    void SetProperty(IEnumerable<T> entities, IList<IEntityReader> resultSets);
}

class IncludeResolver<T, U, TKey>:IIncludeResolver<T>{
    int _resultSetIdx;
    Action<T, U[]> _set;
    Func<T, TKey> _foreignSelector;
    Func<U, TKey> _primarySelector;

    public IncludeResolver(Action<T, U[]> set,
                            Func<T, TKey> foreignSelector,
                            Func<U, TKey> primarySelector,
                            int resultSetIdx){
        _set = set; 
        _foreignSelector = foreignSelector; 
        _primarySelector = primarySelector; 
        _resultSetIdx = resultSetIdx;
    }


    public void SetProperty(IEnumerable<T> entities, IList<IEntityReader> resultSets){
        var resultSet = resultSets[_resultSetIdx];
        var childByKey = resultSet.Rows.OfType<U>()
                            .ToLookup(r => _primarySelector(r));
        foreach(var e in entities){
            var foreign = _foreignSelector(e);
            if (childByKey.Contains(foreign)){
                _set(e, childByKey[foreign].ToArray());
            }
        }
    }
}

class EntityReader<T>:IEntityReader where T: new(){

    IList<T> _rows = new List<T>();
    IList<IIncludeResolver<T>> _resolvers;
    IList<IPropertySetter<T>> _setters;

    public int ResultSetIndex{get; private set;}

    public EntityReader(IList<IIncludeResolver<T>> resolvers,
                        IList<IPropertySetter<T>> setters, 
                        int resultSetIndex){
        _resolvers = resolvers;
        _setters = setters;
        ResultSetIndex = resultSetIndex;
    }


    public void ReadRow(IDataReader rdr){
        var entity = new T();
        foreach(var setter in _setters){
            setter.SetValue(entity, rdr);
        }
        _rows.Add(entity);
    }
    public void ResolveIncludes(IList<IEntityReader> resultSets){
        foreach(var resolver in _resolvers){
            resolver.SetProperty(_rows, resultSets);
        }
    }
    public IEnumerable<Object> Rows{get{ return (IEnumerable<Object>)_rows; }}
}


public class Parser<TRoot> where TRoot:new(){
    IList<IEntityReader> _entityReaders;

    public Parser(Action<ParserSettings<TRoot>> cfg){
        var ps = new ParserSettings<TRoot>(0);
        cfg(ps);
        _entityReaders = ps.CreateReaders().ToArray();
    }

    public IEnumerable<TRoot> Parse(IDataReader rdr){
        var resultSetIdx = 0;

        do {
            var er = _entityReaders[resultSetIdx];
            if (er != null){
                while(rdr.Read()){
                    er.ReadRow(rdr);
                }
            }
            resultSetIdx++;
        }while(rdr.NextResult());

        var root = _entityReaders[0];
        root.ResolveIncludes(_entityReaders);
        return root.Rows.OfType<TRoot>();
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
                var parser = new Parser<User>(_ => 
                                    _.IncludeList(u => u.Posts,
                                                  u => u.UserId,
                                                  p => p.UserId, 1));
                var usersWithPosts = parser.Parse(rdr).ToArray();
                using (var str = new MemoryStream()){
                    new DataContractJsonSerializer(typeof(User[])).WriteObject(str, usersWithPosts);
                    Console.WriteLine(Encoding.UTF8.GetString(str.ToArray()));
                }
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