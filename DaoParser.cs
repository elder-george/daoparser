using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Data;
using System.Reflection;


namespace DaoParser {
    
    public interface IParserSettings{
        IEnumerable<IEntityReaderTemplate> CreateReaderTemplates();
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

        public ParserSettings<U> Include<TProp, U, TKey>(
                Expression<Func<T, TProp>> prop, 
                Expression<Func<T, TKey>> foreign,
                Expression<Func<U, TKey>> primary, 
                Func<IEnumerable<U>, TProp> convertToPropertyValue,
                int resultSetIdx) where U:new()
        {
            var member = (MemberExpression)prop.Body;
            var propInfo = (PropertyInfo)member.Member;
            _resolvers[propInfo] = CreateResolver(prop, foreign, primary, convertToPropertyValue, resultSetIdx);
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

        public IEnumerable<IEntityReaderTemplate> CreateReaderTemplates(){
            var readers = new List<IEntityReaderTemplate>();
            var setters = new List<IPropertySetterTemplate<T>>();
            var resolvers = new List<IIncludeResolver<T>>();
            foreach(var prop in typeof(T).GetProperties()){
                if (_resolvers.ContainsKey(prop)){
                    resolvers.Add(_resolvers[prop]);
                } else if (!_ignores.Contains(prop)){
                    setters.Add(CreateSetterTemplate(prop));
                }
            }
            readers.Add(new EntityReaderTemplate<T>(resolvers, setters, _resultSetIdx));
            foreach(var include in _includes){
                readers.AddRange(include.CreateReaderTemplates());
            }
            return readers;
        }

        IIncludeResolver<T> CreateResolver<TProp, U, TKey>(
                Expression<Func<T, TProp>> prop, 
                Expression<Func<T, TKey>> foreign,
                Expression<Func<U, TKey>> primary, 
                Func<IEnumerable<U>, TProp> convertToPropertyValue,
                int resultSetIdx){
            var param = Expression.Parameter(typeof(TProp));
            return new IncludeResolver<T, TProp, U, TKey>(
                Expression.Lambda<Action<T, TProp>>(
                    Expression.Assign(prop.Body, param), 
                    new ParameterExpression[]{prop.Parameters[0], param}).Compile(),
                foreign.Compile(),
                primary.Compile(),
                convertToPropertyValue,
                resultSetIdx);
        }

        IPropertySetterTemplate<T> CreateSetterTemplate(PropertyInfo prop){
            var propType = prop.PropertyType;
            var actionType = typeof(Action<,>).MakeGenericType(typeof(T), propType);
            var set = Delegate.CreateDelegate(actionType,prop.GetSetMethod());
            var convertType = typeof(Func<,>).MakeGenericType(typeof(object), propType);
            Delegate convert;

            /*var entityParam = Expression.Parameter(typeof(T));
            var valueParam = Expression.Parameter(propType);
            var setMethod = Expression.Lambda(actionType, 
                                Expression.Assign(
                                    Expression.MakeMemberAccess(entityParam, prop),
                                    valueParam), new[]{entityParam,valueParam});*/

            if (propType.IsValueType && propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>)){
                var valueType = propType.GetGenericArguments()[0];
                convert = Delegate.CreateDelegate(convertType, 
                                            this.GetType().GetMethod("ConvertNullable", BindingFlags.Static|BindingFlags.Public).MakeGenericMethod(valueType));
            } else {
                convert = Delegate.CreateDelegate(convertType, 
                    this.GetType().GetMethod("Convert", BindingFlags.Static|BindingFlags.Public)
                        .MakeGenericMethod(propType));
            }
            return (IPropertySetterTemplate<T>)Activator.CreateInstance(
                        typeof(PropertySetterTemplate<,>).MakeGenericType(typeof(T), propType),
                        new object[]{ 
//                        setMethod.Compile(), 
                        set,
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

    public interface IEntityReaderTemplate{
        int ResultSetIndex{get;}
        IEntityReader Specialize(IDataReader reader);
    }

    public interface IEntityReader{
        int ResultSetIndex{get;}
        object ReadRow(IDataRecord row);
        void ResolveIncludes(IList<IList<object>> resultSets);
    }

    interface IPropertySetterTemplate<T>{
        IPropertySetter<T> Specialize(IDictionary<string, int> columns);
    }

    class PropertySetterTemplate<T, U>: IPropertySetterTemplate<T>{
        Action<T, U> _set;
        Func<object, U> _convert;
        string _propertyName;
        bool _ignoreMisses;

        public PropertySetterTemplate(Action<T, U> set, Func<object, U> convert, string propertyName, bool ignoreMisses){
            _set = set;
            _convert = convert;
            _propertyName = propertyName;
            _ignoreMisses = ignoreMisses;
        }

        public IPropertySetter<T> Specialize(IDictionary<string, int> columns){
            int idx;
            if (columns.TryGetValue(_propertyName, out idx)){
                return new PropertySetter<T,U>(_set, _convert, idx);
            } else if (_ignoreMisses){
                return new DummySetter<T>();
            } else throw new ArgumentException("columns", String.Format("Column {0} is missing", _propertyName));
        }
    }

    interface IPropertySetter<T>{
        void SetValue(T target, IDataRecord row);
    }

    class DummySetter<T>: IPropertySetter<T>{
        public void SetValue(T target, IDataRecord row){
            /* DO NOTHING */
        }
    }

    class PropertySetter<T, U>: IPropertySetter<T>{
        Action<T, U> _set;
        Func<object, U> _convert;
        int _columnIndex;

        public PropertySetter(Action<T, U> set, Func<object, U> convert, int columnIndex){
            _set = set;
            _convert = convert;
            _columnIndex = columnIndex;
        }

        public void SetValue(T entity, IDataRecord row){
            _set(entity, _convert(row[_columnIndex]));
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

    class EntityReaderTemplate<T>: IEntityReaderTemplate where T:new(){
        IList<IIncludeResolver<T>> _resolvers;
        IList<IPropertySetterTemplate<T>> _setterTemplates;
        public int ResultSetIndex{get; private set;}

        public EntityReaderTemplate(IList<IIncludeResolver<T>> resolvers,
                            IList<IPropertySetterTemplate<T>> setterTemplates, 
                            int resultSetIndex){
            _resolvers = resolvers;
            _setterTemplates = setterTemplates;
            ResultSetIndex = resultSetIndex;
        }

        public IEntityReader Specialize(IDataReader reader){
            var dictionary = BuildColumnMapping(reader);

            var setters = _setterTemplates.Select(st => st.Specialize(dictionary)).ToList();

            return new EntityReader<T>(_resolvers, setters, ResultSetIndex);
        }

        IDictionary<string, int> BuildColumnMapping(IDataReader rdr){
            var columns = new Dictionary<string, int>(rdr.FieldCount);
            for(var i = 0; i < rdr.FieldCount; i++){
                columns[rdr.GetName(i)] = i;
            }
            return columns;
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

        public object ReadRow(IDataRecord row){
            var entity = new T();
            foreach(var setter in _setters){
                setter.SetValue(entity, row);
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

    public static class ParserSettingsExtensions{
        // Due to C# limitations we have to specify separate conversion for each collection type.
        public static ParserSettings<U> IncludeList<T, U, TKey>(
                this ParserSettings<T> self,
                Expression<Func<T, List<U>>> listProp, 
                Expression<Func<T, TKey>> foreign,
                Expression<Func<U, TKey>> primary, 
                int resultSetIdx) 
                    where T:new()
                    where U:new(){
            return self.Include<List<U>, U, TKey>(listProp, foreign, primary, 
                        children => {
                            var collection = new List<U>();
                            foreach(var item in children){
                                collection.Add(item);
                            }
                            return collection;
                        },
                        resultSetIdx);
        }

        public static ParserSettings<U> IncludeList<T, U, TKey>(
                this ParserSettings<T> self,
                Expression<Func<T, U[]>> listProp, 
                Expression<Func<T, TKey>> foreign,
                Expression<Func<U, TKey>> primary, 
                int resultSetIdx) 
                    where T:new()
                    where U:new() {
            return self.Include<U[], U, TKey>(listProp, foreign, primary, c => c.ToArray(), resultSetIdx);
        }

        public static ParserSettings<U> IncludeSingle<T, U, TKey>(
                this ParserSettings<T> self,
                Expression<Func<T, U>> prop, 
                Expression<Func<T, TKey>> foreign,
                Expression<Func<U, TKey>> primary, 
                int resultSetIdx) 
                    where T:new()
                    where U:new(){
            return self.Include<U, U, TKey>(prop, foreign, primary, c => c.Single(), resultSetIdx);
        }
    }

    public class Parser<TRoot> where TRoot:new(){
        IList<IEntityReaderTemplate> _entityReaderTemplates;

        public Parser(Action<ParserSettings<TRoot>> cfg){
            var ps = new ParserSettings<TRoot>(0);
            cfg(ps);
            _entityReaderTemplates = ps.CreateReaderTemplates()
                                        .ToLookup(r => r.ResultSetIndex)
                                        .Select(g=>g.FirstOrDefault()).OrderBy(r=>r.ResultSetIndex).ToArray();
        }

        public IEnumerable<TRoot> Parse(IDataReader rdr){
            var resultSetIdx = 0;
            var parsedRowSets = new List<IList<object>>(_entityReaderTemplates.Count);
            
            var readers = new List<IEntityReader>(_entityReaderTemplates.Count);
            do {
                var ert = _entityReaderTemplates[resultSetIdx];
                if (ert != null){
                    var er = ert.Specialize(rdr);
                    readers.Add(er);
                    var rowset = new List<Object>();
                    while(rdr.Read()){
                        rowset.Add(er.ReadRow(rdr));
                    }
                    parsedRowSets.Add(rowset);
                }
                resultSetIdx++;
            }while(rdr.NextResult());

            foreach(var reader in readers){
                reader.ResolveIncludes(parsedRowSets);
            }
            return parsedRowSets[0].OfType<TRoot>();
        }
    }
}
