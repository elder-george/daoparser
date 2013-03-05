using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Data;
using System.Reflection;


namespace DaoParser {
    
    public interface IParserSettings{
        IEnumerable<IEntityReader> CreateReaders();
    }

    public class ParserSettings<T>: IParserSettings where T:new(){
        IDictionary<PropertyInfo, IIncludeResolver<T>> _resolvers = new Dictionary<PropertyInfo, IIncludeResolver<T>>();
        IList<IParserSettings> _includes = new List<IParserSettings>();
        bool _ignoreAllMisses;

        HashSet<PropertyInfo> _ignores = new HashSet<PropertyInfo>();
        int _resultSetIdx;

        public ParserSettings(int resultSetIdx){
            _resultSetIdx  = resultSetIdx;
        }

        public ParserSettings<U> IncludeList<U, TKey>(
                Expression<Func<T, U[]>> listProp, 
                Expression<Func<T, TKey>> foreign,
                Expression<Func<U, TKey>> primary, 
                int resultSetIdx) where U:new(){
            var member = (MemberExpression)listProp.Body;
            var prop = (PropertyInfo)member.Member;
            _resolvers[prop] = CreateResolver(listProp, foreign, primary, resultSetIdx);
            var childSettings = new ParserSettings<U>(resultSetIdx);
            _includes.Add(childSettings); //TODO: if they go nested then overlaps are possible!!!
            return childSettings;
        }

        public ParserSettings<U> IncludeSingle<U, TKey>(
                Expression<Func<T, U>> prop, 
                Expression<Func<T, TKey>> foreign,
                Expression<Func<U, TKey>> primary, 
                int resultSetIdx) where U:new(){
            // It's basically the same as IncludeList excepting Expression argument. Need reduce duplication
            var member = (MemberExpression)prop.Body;
            var propInfo = (PropertyInfo)member.Member;
            _resolvers[propInfo] = CreateResolver(prop, foreign, primary, resultSetIdx);
            var childSettings = new ParserSettings<U>(resultSetIdx);
            _includes.Add(childSettings); //TODO: if they go nested then overlaps are possible!!!
            return childSettings;
        }

        public void IgnoreAllMisses(){
            _ignoreAllMisses = true;
        }

        public void Ignore<U>(Expression<Func<T, U>> propExpr){
            var member = (MemberExpression) propExpr.Body;
            _ignores.Add((PropertyInfo)member.Member);
        }

        public IEnumerable<IEntityReader> CreateReaders(){
            var readers = new List<IEntityReader>();
            var setters = new List<IPropertySetter<T>>();
            var resolvers = new List<IIncludeResolver<T>>();
            foreach(var prop in typeof(T).GetProperties()){
                if (_resolvers.ContainsKey(prop)){
                    resolvers.Add(_resolvers[prop]);
                } else if (!_ignores.Contains(prop)){
                    setters.Add(CreateSetter(prop));
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
                Expression<Func<U, TKey>> primary, int resultSetIdx){
        
            var param = Expression.Parameter(typeof(U[]));
            return new IncludeResolver<T, U[], U, TKey>(
                Expression.Lambda<Action<T, U[]>>(
                    Expression.Assign(listProp.Body, param), 
                    new ParameterExpression[]{listProp.Parameters[0], param}).Compile(),
                foreign.Compile(),
                primary.Compile(),
                children => children.ToArray(),
                resultSetIdx);
        }

        IIncludeResolver<T> CreateResolver<U, TKey>(
                Expression<Func<T, U>> listProp, 
                Expression<Func<T, TKey>> foreign,
                Expression<Func<U, TKey>> primary, int resultSetIdx){
        
            var param = Expression.Parameter(typeof(U));
            return new IncludeResolver<T, U, U, TKey>(
                Expression.Lambda<Action<T, U>>(
                    Expression.Assign(listProp.Body, param), 
                    new ParameterExpression[]{listProp.Parameters[0], param}).Compile(),
                foreign.Compile(),
                primary.Compile(),
                children => children.Single(),
                resultSetIdx);
        }


        IPropertySetter<T> CreateSetter(PropertyInfo prop){
            var propType = prop.PropertyType;
            var actionType = typeof(Action<,>).MakeGenericType(typeof(T), propType);
            var set = Delegate.CreateDelegate(actionType,prop.GetSetMethod());
            var convertType = typeof(Func<,>).MakeGenericType(typeof(object), propType);
            Delegate convert;

            var entityParam = Expression.Parameter(typeof(T));
            var valueParam = Expression.Parameter(propType);
            var setMethod = Expression.Lambda(actionType, 
                                Expression.Assign(
                                    Expression.MakeMemberAccess(entityParam, prop),
                                    valueParam), new[]{entityParam,valueParam});

            if (propType.IsValueType && propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>)){
                var valueType = propType.GetGenericArguments()[0];
                convert = Delegate.CreateDelegate(convertType, 
                                            this.GetType().GetMethod("ConvertNullable", BindingFlags.Static|BindingFlags.Public).MakeGenericMethod(valueType));
            } else {
                convert = Delegate.CreateDelegate(convertType, 
                    this.GetType().GetMethod("Convert", BindingFlags.Static|BindingFlags.Public)
                        .MakeGenericMethod(propType));
            }
            return (IPropertySetter<T>)Activator.CreateInstance(
                        typeof(PropertySetter<,>).MakeGenericType(typeof(T), propType),
                        new object[]{ 
                        setMethod.Compile(), 
                        //set,
                        convert, prop.Name, _ignoreAllMisses}, 
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
        object ReadRow(IDictionary<string,int> columns, IDataRecord row);
        void ResolveIncludes(IList<IList<object>> resultSets);
    }

    interface IPropertySetter<T>{
        void SetValue(T target, IDictionary<string, int> columns, IDataRecord row);
    }

    class PropertySetter<T, U>: IPropertySetter<T>{
        Action<T, U> _set;
        Func<object, U> _convert;
        string _propertyName;
        bool _ignoreMisses;

        public PropertySetter(Action<T, U> set, Func<object, U> convert, string propertyName, bool ignoreMisses){
            _set = set;
            _convert = convert;
            _propertyName = propertyName;
            _ignoreMisses = ignoreMisses;
        }

        public void SetValue(T entity, IDictionary<string, int> columns, IDataRecord row){
            int idx;
            if (columns.TryGetValue(_propertyName, out idx))
                _set(entity, _convert(row[idx]));
            else if (!_ignoreMisses){
                throw new IndexOutOfRangeException(_propertyName);
            }
        }
    }

    interface IIncludeResolver<T>{
        void SetProperty(IEnumerable<T> entities, IList<IList<object>> resultSets);
    }

    class IncludeResolver<TParent, TProperty, TChild, TKey>: IIncludeResolver<TParent>{
        int _resultSetIdx;
        Action<TParent, TProperty> _set;
        Func<TParent, TKey> _foreignSelector;
        Func<TChild, TKey> _primarySelector;
        Func<IEnumerable<TChild>, TProperty> _adapt;

        public IncludeResolver(Action<TParent, TProperty> set,
                Func<TParent, TKey> foreignSelector,
                Func<TChild, TKey> primarySelector,
                Func<IEnumerable<TChild>, TProperty> adapt,
                int resultSetIdx){
            _set = set;
            _foreignSelector = foreignSelector;
            _primarySelector = primarySelector;
            _adapt = adapt;
            _resultSetIdx = resultSetIdx;
        }
        public void SetProperty(IEnumerable<TParent> entities, IList<IList<object>> resultSets){
            var resultSet = resultSets[_resultSetIdx];
            var childByKey = resultSet.OfType<TChild>()
                                .ToLookup(r => _primarySelector(r));
            foreach(var e in entities){
                var foreign = _foreignSelector(e);
                if (childByKey.Contains(foreign)){
                    _set(e, _adapt(childByKey[foreign]));
                }
            }
        }
    }

    class EntityReader<T>:IEntityReader where T: new(){
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

        public object ReadRow(IDictionary<string, int> columns, IDataRecord row){
            var entity = new T();
            foreach(var setter in _setters){
                setter.SetValue(entity, columns, row);
            }
            return entity;
        }
        public void ResolveIncludes(IList<IList<object>> resultSets){
            var rows = resultSets[ResultSetIndex].OfType<T>();
            foreach(var resolver in _resolvers){
                resolver.SetProperty(rows, resultSets);
            }
        }
    }

    public class Parser<TRoot> where TRoot:new(){
        IList<IEntityReader> _entityReaders;

        public Parser(Action<ParserSettings<TRoot>> cfg){
            var ps = new ParserSettings<TRoot>(0);
            cfg(ps);
            _entityReaders = ps.CreateReaders().ToLookup(r => r.ResultSetIndex).Select(g=>g.FirstOrDefault()).OrderBy(r=>r.ResultSetIndex).ToArray();
        }

        IDictionary<string, int> BuildColumnMapping(IDataReader rdr){
            var columns = new Dictionary<string, int>(rdr.FieldCount);
            for(var i = 0; i < rdr.FieldCount; i++){
                columns[rdr.GetName(i)] = i;
            }
            return columns;
        }

        public IEnumerable<TRoot> Parse(IDataReader rdr){
            var resultSetIdx = 0;
            var parsedRowSets = new List<IList<object>>(_entityReaders.Count);
            
            do {
                var columns = BuildColumnMapping(rdr);
                var er = _entityReaders[resultSetIdx];
                if (er != null){
                    var rowset = new List<Object>();
                    while(rdr.Read()){
                        rowset.Add(er.ReadRow(columns, rdr));
                    }
                    parsedRowSets.Add(rowset);
                }
                resultSetIdx++;
            }while(rdr.NextResult());

            foreach(var reader in _entityReaders){
                reader.ResolveIncludes(parsedRowSets);
            }
            return parsedRowSets[0].OfType<TRoot>();
        }
    }
}
