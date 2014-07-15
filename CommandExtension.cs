using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace SqlPipe
{
    public static partial class CommandExtension
    {
        private static readonly Regex Regex = new Regex("@[1-9a-zA-Z#_]+", RegexOptions.None);

        public static List<T> ReadAll<T>(this Command command, Dictionary<string, object> paras = null,
            string[] includes = null, IDbTransaction transaction = null)
        {
            var result = new List<T>();
            Type conversionType = typeof (T);
            if (conversionType.IsArray)
            {
                return Command(command, (dbCommand, list) =>
                {
                    IDataReader reader = dbCommand.ExecuteReader();
                    int count = reader.FieldCount;
                    Type elementType = typeof (T).GetElementType();
                    while (reader.Read())
                    {
                        Array array = Array.CreateInstance(elementType, count);
                        for (int index = 0; index < count; index++)
                            array.SetValue(reader[index], index);
                        list.Add((T) (object) array);
                    }
                    return list;
                }, new List<T>(), paras, transaction);
            }
            if (conversionType.IsClass && conversionType != typeof (string))
            {
                var hashtables = new Dictionary<string, Hashtable>();
                if (includes != null && includes.Length > 0)
                {
                    foreach (string include in includes)
                    {
                        string[] strings = include.Split(".".ToCharArray());
                        if (strings.Length > 1)
                        {
                            if (!hashtables.ContainsKey(strings[1]))
                                hashtables.Add(strings[1], new Hashtable());
                        }
                        else
                        {
                            if (!hashtables.ContainsKey("@Key"))
                                hashtables.Add("@Key", new Hashtable());
                        }
                    }
                }


                PreRead<List<List<PropertyInfo>>> preRead = schemaTable => SchemaList(schemaTable, typeof (T));
                ReadData<List<List<PropertyInfo>>> readData =
                    (reader, list) =>
                    {
                        var readToObject = ReadToObject<T>(reader, list);
                        foreach (var hashtable1 in hashtables)
                        {
                            object value = hashtable1.Key == "@Key" ? reader[0] : reader[hashtable1.Key];
                            if (!hashtable1.Value.Contains(value))
                                hashtable1.Value.Add(value, readToObject);
                        }

                        result.Add(readToObject);
                    };
                if (includes != null && includes.Length > 0)
                {
                    Command(command, dbCommand =>
                    {
                        IDataReader reader = dbCommand.ExecuteReader();
                        List<List<PropertyInfo>> data = preRead(reader.GetSchemaTable());
                        while (reader.Read())
                        {
                            readData(reader, data);
                        }

                        foreach (string name in includes)
                        {
                            if (!reader.NextResult()) break;

                            string[] strings = name.Split(".".ToCharArray());
                            PropertyInfo propertyInfo = conversionType.GetProperty(strings[0]);
                            Type type = propertyInfo.PropertyType.GetGenericArguments()[0];
                            PreRead<List<List<PropertyInfo>>> preRead2 =
                                schemaTable => SchemaList(schemaTable, type);
                            ReadData<List<List<PropertyInfo>>> readData2 = (dataReader, list) =>
                            {
                                object key =
                                    dataReader[1];

                                string s = strings.Length > 1
                                    ? strings[1]
                                    : "@Key";
                                object obj =
                                    hashtables[s][key];
                                if (obj == null) return;
                                var o =
                                    propertyInfo
                                        .GetValue(obj,
                                            new object[0
                                                ]) as
                                        IList;
                                if (o == null)
                                {
                                    o =
                                        Activator
                                            .CreateInstance
                                            (propertyInfo
                                                .PropertyType)
                                            as IList;
                                    propertyInfo
                                        .SetValue(obj, o,
                                            new object[0
                                                ]);
                                }
                                o.Add(
                                    ReadToObject(
                                        dataReader, list,
                                        type));
                            };

                            List<List<PropertyInfo>> data2 = preRead2(reader.GetSchemaTable());
                            while (reader.Read())
                            {
                                readData2(reader, data2);
                            }
                        }
                    }, paras, transaction);
                }
                else
                {
                    ReadOne(command, preRead, readData, paras, false, transaction);
                }
            }
            else
            {
                List<T> t = result;
                ReadData readData = reader => GetValue(t, reader, conversionType);
                ReadOne(command, readData, paras, false,
                    transaction);
            }

            return result;
        }

        private static void GetValue<T>(List<T> t, IDataReader reader, Type conversionType)
        {
            t.Add((T) Convert.ChangeType(reader[0], conversionType));
        }

        public static PagedList<T> ReadPage<T>(this Command command, int pageNo, int pageSize,
            Dictionary<string, object> paras = null, IDbTransaction transaction = null)
        {
            int totalItemCount = 0;
            var result = new List<T>();
            ReadData<List<List<PropertyInfo>>> readData = (reader, list) => result.Add(ReadToObject<T>(reader, list));
            PreRead<List<List<PropertyInfo>>> preRead = schemaTable => SchemaList(schemaTable, typeof (T));
            var dictionary = new Dictionary<string, object>
            {
                {"@StartRowNum", (pageNo - 1)*pageSize + 1},
                {"@EndRowNum", pageNo*pageSize}
            };
            if (paras != null)
                foreach (var pair in paras)
                {
                    dictionary.Add(pair.Key, pair.Value);
                }

            Command(command, dbCommand =>
            {
                IDataReader reader = dbCommand.ExecuteReader();
                reader.Read();
                totalItemCount = Convert.ToInt32(reader[0]);
                reader.NextResult();
                List<List<PropertyInfo>> read = preRead(reader.GetSchemaTable());
                while (reader.Read())
                {
                    readData(reader, read);
                }
            }, dictionary, transaction);

            return new PagedList<T>(result, pageNo, pageSize, totalItemCount);
        }

        /// <summary>
        ///     读取树形结构
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <param name="paras"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public static List<T> ReadTree<T>(this Command command, Dictionary<string, object> paras = null,
            IDbTransaction transaction = null)
        {
            var result = new List<T>();
            var nodes = new Dictionary<int, T>();
            var remain = new List<T>();
            const int item = 0;
            Type type = typeof (T);
            ReadOne(command, schemaTable =>
            {
                List<string> columns =
                    (from DataRow dataRow in schemaTable.Rows select dataRow["ColumnName"].ToString())
                        .ToList();

                List<List<PropertyInfo>> list = SchemaList(schemaTable, type);
                int parent = columns.IndexOf("ParentId");
                return new {parent, list};
            }, (reader, data) =>
            {
                int parent = data.parent;
                List<List<PropertyInfo>> list = data.list;
                var t = ReadToObject<T>(reader, list);

                int itemId = reader.GetInt32(item);
                int parentId = reader.GetInt32(parent);

                if (parentId == 0)
                {
                    nodes.Add(itemId, t);
                    result.Add(t);
                }
                else
                {
                    if (nodes.ContainsKey(parentId))
                    {
                        var o = ((dynamic) nodes[parentId]);
                        try
                        {
                            if (o.Children == null)
                                o.Children = new List<T>();
                            o.Children.Add(t);
                            nodes.Add(itemId, t);
                        }
                        catch (Exception ex)
                        {
                            ex.Process();
                        }
                    }
                    else
                    {
                        remain.Add(t);
                    }
                }
            }, paras, false, transaction);

            while (remain.Count > 0)
            {
                var finds = new List<T>();

                foreach (T t in remain)
                {
                    dynamic parentId = ((dynamic) t).ParentId;
                    var itemId = (int) typeof (T).GetProperty(typeof (T).Name + "Id").GetValue(t, new object[0]);
                    if (!nodes.ContainsKey(parentId)) continue;
                    dynamic o = nodes[parentId];
                    if (o.Children == null)
                        o.Children = new List<T>();
                    o.Children.Add(t);
                    finds.Add(t);
                    nodes.Add(itemId, t);
                }

                if (finds.Count == 0) break;
                remain.RemoveAll(finds.Contains);
            }
            return result;
        }

        private static T ReadToObject<T>(IDataReader reader, List<List<PropertyInfo>> list)
        {
            var result = Activator.CreateInstance<T>();
            ReadToObject(reader, list, result);
            return result;
        }

        private static object ReadToObject(IDataReader reader, List<List<PropertyInfo>> list, Type type)
        {
            object result = Activator.CreateInstance(type);
            ReadToObject(reader, list, result);
            return result;
        }

        private static void ReadToObject(IDataReader reader, List<List<PropertyInfo>> list, object t)
        {
            foreach (var property in list)
            {
                if (property == null || property.Count == 0) break;
                string name = string.Join("_", property.Select(c => c.Name));
                object o = reader[name];
                if (o is DBNull) continue;
                object obj = t;

                for (int index = 0; index < property.Count; index++)
                {
                    PropertyInfo propertyInfo = property[index];

                    Type propertyType = propertyInfo.PropertyType;

                    if (propertyType.IsGenericType &&
                        propertyType.GetGenericTypeDefinition() == typeof (Nullable<>))
                    {
                        propertyType = propertyType.GetGenericArguments()[0];
                    }
                    if (index == property.Count - 1)
                    {
                        SetPropertyValue(obj, o, propertyInfo);
                    }
                    else
                    {
                        object propretyValue = propertyInfo.GetValue(obj, new object[0]);
                        if (propretyValue == null)
                        {
                            object instance = Activator.CreateInstance(propertyType);
                            propertyInfo.SetValue(obj, instance, new object[0]);
                            obj = instance;
                        }
                        else
                        {
                            obj = propretyValue;
                        }
                    }
                }
            }
        }

        /// <summary>
        ///     根据Sql解析参数
        /// </summary>
        /// <param name="command"></param>
        /// <param name="paras"></param>
        /// <param name="prefix"></param>
        /// <returns></returns>
        private static Dictionary<string, object> ToDictionary(Command command, object paras, string prefix = "@")
        {
            if (paras == null)
                return new Dictionary<string, object>();
            MatchCollection matches = Regex.Matches(command.Text);

            List<string> paraNames = (from ma in matches.Cast<Match>() select ma.Value).Distinct().ToList();

            if (paraNames.Count == 0)
            {
                Dictionary<string, object> objects =
                    paras.GetType()
                        .GetProperties()
                        .Where(propertyInfo => propertyInfo.CanRead)
                        .ToDictionary(propertyInfo => prefix + propertyInfo.Name,
                            propertyInfo => propertyInfo.GetValue(paras, new object[0]));

                return objects;
            }

            var dictionary = new Dictionary<string, object>();
            foreach (string s in paraNames)
            {
                object value = paras;
                string[] strings = s.Substring(1).Split('_');
                foreach (string s1 in strings)
                {
                    if (value == null) continue;
                    PropertyInfo propertyInfo = value.GetType().GetProperties().FirstOrDefault(c => c.Name == s1);
                    if (propertyInfo != null)
                    {
                        value = propertyInfo.GetValue(value, new object[0]);

                        if (propertyInfo.PropertyType.IsEnum &&
                            TypeDescriptor.GetConverter(propertyInfo.PropertyType).ToString() !=
                            typeof (EnumConverter).FullName)
                            value = TypeDescriptor.GetConverter(propertyInfo.PropertyType)
                                .ConvertTo(value, typeof (string));
                    }
                    else
                    {
                        goto Ignore;
                    }
                }
                dictionary.Add(s, value);
                Ignore:
                ;
            }


            return dictionary;
        }

        public static T ReadOne<T>(this Command command, Dictionary<string, object> paras = null,
            IDbTransaction transaction = null)
        {
            T result = default(T);
            Type type = typeof (T);
            if (type.IsClass && type != typeof (string))
            {
                if (type.IsArray)
                {
                    ReadOne(command, reader => result = (T) ReadToArray(reader), paras, true, transaction);
                }
                else if (type.GetInterfaces().Contains(typeof (IDictionary)))
                {
                    ReadOne(command, SchemaColumns, (reader, list) => { result = ReadToDictionary<T>(reader, list); },
                        paras, true, transaction);
                }
                else
                {
                    ReadOne(command, schemaTable => SchemaList(schemaTable, type),
                        (reader, list) => { result = ReadToObject<T>(reader, list); }, paras, true, transaction);
                }
            }
            else
            {
                Command(command, dbCommand => Result(dbCommand, out result), paras, transaction);
            }

            return result;
        }

        private static void Result<T>(IDbCommand dbCommand, out T result)
        {
            object scalar = dbCommand.ExecuteScalar();

            result = scalar is DBNull
                ? default(T)
                : (scalar.GetType() == typeof (T)
                    ? (T) scalar
                    : (T) Convert.ChangeType(scalar, typeof (T)));
        }

        private static object ReadToArray(IDataReader reader)
        {
            var array = new object[reader.FieldCount];
            for (int index = 0; index < array.Length; index++)
            {
                object data = reader[index];

                array.SetValue(data != DBNull.Value ? data : null, index);
            }
            return array;
        }

        private static T ReadToDictionary<T>(IDataReader reader, List<string> list)
        {
            var instance = Activator.CreateInstance<T>();
            var a = instance as IDictionary;
            foreach (string s in list)
            {
                if (a != null) a.Add(s, reader[s]);
            }
            return instance;
        }

        private static List<List<PropertyInfo>> SchemaList(DataTable schemaTable, Type type)
        {
            List<string> clumns = SchemaColumns(schemaTable);
            List<PropertyInfo> propertyInfos = Array.FindAll(type.GetProperties(), c => c.CanRead).ToList();
            IEnumerable<List<PropertyInfo>> propertyLists =
                clumns.Select(column => GetPropertyList(column, propertyInfos)).Where(t => t != null && t.Count > 0);
            return propertyLists.ToList();
        }

        private static List<string> SchemaColumns(DataTable schemaTable)
        {
            return SchemaColumns(schemaTable, true);
        }

        private static List<string> SchemaColumns(DataTable schemaTable, bool toLower)
        {
            IEnumerable<string> columnNames = from DataRow row in schemaTable.Rows
                select toLower ? row["ColumnName"].ToString().ToLower() : row["ColumnName"].ToString();
            return columnNames.ToList();
        }

        private static List<PropertyInfo> GetPropertyList(string column, List<PropertyInfo> propertyInfos)
        {
            bool contains = column.Contains("_");
            var infos = new List<PropertyInfo>();
            if (contains)
            {
                int indexOf = column.IndexOf("_", StringComparison.Ordinal);
                PropertyInfo propertyInfo =
                    propertyInfos.Find(
                        c =>
                            string.Compare(c.Name, column.Substring(0, indexOf),
                                StringComparison.CurrentCultureIgnoreCase) == 0);
                if (propertyInfo == null) return infos;
                infos.Add(propertyInfo);
                string substring = column.Substring(indexOf + 1);
                List<PropertyInfo> list =
                    Array.FindAll(propertyInfo.PropertyType.GetProperties(), c => c.CanRead).ToList();
                List<PropertyInfo> propertyList = GetPropertyList(substring, list);
                if (propertyList.Count == 0)
                    return propertyList;
                infos.AddRange(propertyList);
            }
            else
            {
                PropertyInfo propertyInfo =
                    propertyInfos.Find(c => string.Compare(c.Name, column, StringComparison.OrdinalIgnoreCase) == 0);
                if (propertyInfo != null)
                    infos.Add(propertyInfo);
            }

            return infos;
        }

        private static void SetPropertyValue(this object t, object o, PropertyInfo propertyInfo)
        {
            if (o is DBNull || o == null || !propertyInfo.CanWrite) return;

            Type conversionType = propertyInfo.PropertyType;
            if (conversionType.IsArray)
            {
                const StringSplitOptions options = StringSplitOptions.RemoveEmptyEntries;
                string[] strings = o.ToString().Split(new[] {','}, options);
                Type elementType = conversionType.GetElementType();
                Array instance = Array.CreateInstance(elementType, strings.Length);
                for (int index = 0; index < strings.Length; index++)
                {
                    string s = strings[index];
                    if (!string.IsNullOrEmpty(s))
                        instance.SetValue(Convert.ChangeType(s, elementType), index);
                }
                propertyInfo.SetValue(t, instance, new object[0]);
                return;
            }

            if (conversionType.IsGenericType
                && conversionType.GetGenericTypeDefinition() == typeof (List<>))
            {
                const StringSplitOptions options = StringSplitOptions.RemoveEmptyEntries;
                string[] strings = o.ToString().Split(new[] {','}, options);
                Type elementType = conversionType.GetGenericArguments()[0];

                var instance = Activator.CreateInstance(conversionType) as IList;
                foreach (string s in strings)
                {
                    if (!string.IsNullOrEmpty(s))
                        instance.Add(Convert.ChangeType(s, elementType));
                }
                propertyInfo.SetValue(t, instance, new object[0]);
                return;
            }


            if (conversionType.IsGenericType &&
                conversionType.GetGenericTypeDefinition() == typeof (Nullable<>))
                conversionType = conversionType.GetGenericArguments()[0];

            object value = conversionType.IsEnum
                ? ((TypeDescriptor.GetConverter(propertyInfo.PropertyType).ToString() != typeof (EnumConverter).FullName)
                    ? TypeDescriptor.GetConverter(propertyInfo.PropertyType).ConvertFrom(o)
                    : Enum.ToObject(conversionType, o))
                : Convert.ChangeType(o, conversionType);
            propertyInfo.SetValue(t, value, new object[0]);
        }


        public static void ReadDictionary(this Command command, IDictionary result,Dictionary<string, object> paras = null,
            IDbTransaction transaction = null) 
        {
             
            Type keyType = null;
            Type valueType = null;
            Type type =result.GetType();
            bool needExtend = false;
            if (type.IsGenericType)
            {
                Type[] arguments = type.GetGenericArguments();
                if (arguments.Length == 2)
                {
                    keyType = arguments[0];
                    valueType = arguments[1];
                    if (arguments[1].IsClass && arguments[1] != typeof (string))
                        needExtend = true;
                }
            }
            if (!needExtend)
            {
                ReadOne(command, reader =>
                {
                    try
                    {
                        object key = keyType == null ? reader[0] : Convert.ChangeType(reader[0], keyType);
                        if (result.Contains(key)) return;
                        object value = valueType == null
                            ? reader[1]
                            : Convert.ChangeType(reader[1], valueType);
                        result.Add(key, value);
                    }
                    catch (Exception ex)
                    {
                        ex.Process();
                    }
                }, paras, false, transaction);
            }
            else
            {
                ReadData<List<List<PropertyInfo>>> readData = (reader, list) =>
                {
                    try
                    {
                        object key = reader[0];
                        if (result.Contains(key)) return;
                        object instance = Activator.CreateInstance(valueType);
                        ReadToObject(reader, list, instance);
                        result.Add(key, instance);
                    }
                    catch (Exception ex)
                    {
                        ex.Process();
                    }
                };
                ReadOne(command, schemaTable => SchemaList(schemaTable, valueType), readData, paras, false, transaction);
            }
            
        }

        /// <summary>
        ///     执行
        /// </summary>
        /// <param name="command"></param>
        /// <param name="paras"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public static bool Exec(this Command command, Dictionary<string, object> paras = null,
            IDbTransaction transaction = null)
        {
            return Command(
                command,
                (dbCommand, result) => dbCommand.ExecuteNonQuery() > 0,
                false,
                paras,
                transaction);
        }

        /// <summary>
        /// </summary>
        /// <param name="command"></param>
        /// <param name="commandProcess"></param>
        /// <param name="paras"></param>
        /// <param name="transaction"></param>
        private static void Command(this Command command, CommandProcess commandProcess,
            Dictionary<string, object> paras = null, IDbTransaction transaction = null)
        {
            IDbConnection connection = transaction != null
                ? transaction.Connection
                : Commands.DbConnection(command.DbName);

            IDbCommand dbCommand = connection.CreateCommand();
            bool precompiled = command.Precompiled && command.CommandType == CommandType.Text;
            StringBuilder stringBuilder = precompiled
                ? new StringBuilder(string.Format("EXEC sp_executesql N'{0}'", command.Text.Replace("'", "''")))
                : null;

            dbCommand.CommandType = command.CommandType;

            if (paras != null && paras.Count > 0)
            {
                foreach (var keyValuePair in paras)
                {
                    IDbDataParameter parameter = dbCommand.CreateParameter();
                    parameter.ParameterName = keyValuePair.Key;
                    parameter.Value = keyValuePair.Value ?? DBNull.Value;
                    dbCommand.Parameters.Add(parameter);
                }
                if (precompiled)
                {
                    stringBuilder.Append(", N'");
                    List<IDbDataParameter> collection = dbCommand.Parameters.Cast<IDbDataParameter>().ToList();

                    string[] strings =
                        collection.ToList<IDataParameter>()
                            .Select(c => string.Format("{0} AS {1}", c.ParameterName, c.DbType))
                            .ToArray();
                    stringBuilder.Append(string.Join(",", strings));
                    stringBuilder.Append("', ");
                    stringBuilder.Append(string.Join(", ", paras.Select(c => c.Key).ToArray()));
                }
            }

            dbCommand.CommandText = !precompiled ? command.Text : stringBuilder.ToString();

            try
            {
                if (transaction != null)
                    dbCommand.Transaction = transaction;
                else
                    connection.OpenIfClose();

                commandProcess(dbCommand);
            }
            catch (Exception ex)
            {
                if (transaction != null)
                    transaction.Rollback();

                ex.Process();
                throw;
            }
            finally
            {
                if (transaction == null)
                    connection.CloseIfOpen();
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="command"></param>
        /// <param name="commandProcess"></param>
        /// <param name="result"></param>
        /// <param name="paras"></param>
        /// <param name="transaction"></param>
        private static T Command<T>(this Command command, CommandProcess<T> commandProcess, T result,
            Dictionary<string, object> paras = null, IDbTransaction transaction = null)
        {
            IDbConnection connection = transaction != null
                ? transaction.Connection
                : Commands.DbConnection(command.DbName);

            IDbCommand dbCommand = connection.CreateCommand();
            dbCommand.CommandText = command.Text;
            dbCommand.CommandType = command.CommandType;

            if (paras != null)
            {
                foreach (var keyValuePair in paras)
                {
                    IDbDataParameter parameter = dbCommand.CreateParameter();
                    parameter.ParameterName = keyValuePair.Key;
                    parameter.Value = keyValuePair.Value ?? DBNull.Value;
                    dbCommand.Parameters.Add(parameter);
                }
            }


            try
            {
                if (transaction != null)
                    dbCommand.Transaction = transaction;
                else
                    connection.OpenIfClose();

                return commandProcess(dbCommand, result);
            }
            catch (Exception ex)
            {
                if (transaction != null)
                    transaction.Rollback();

                ex.Process();
                throw;
            }
            finally
            {
                if (transaction == null)
                    connection.CloseIfOpen();
            }
        }

        private static void ReadOne<T>(this Command command, PreRead<T> preRead, ReadData<T> readData,
            Dictionary<string, object> paras, bool oneRow, IDbTransaction transaction = null)
        {
            Command(command, dbCommand =>
            {
                using (IDataReader reader = dbCommand.ExecuteReader())
                {
                    T data = preRead(reader.GetSchemaTable());
                    while (reader.Read())
                    {
                        readData(reader, data);
                        if (oneRow) break;
                    }

                    reader.Close();
                }
            }, paras, transaction);
        }

        private static void ReadOne(this Command command, ReadData readData, Dictionary<string, object> paras, bool oneRow,
            IDbTransaction transaction = null)
        {
            Command(command, dbCommand =>
            {
                using (IDataReader reader = dbCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        readData(reader);
                        if (oneRow) break;
                    }
                    reader.Close();
                }
            }, paras, transaction);
        }

        /// <summary>
        ///     待改进
        /// </summary>
        /// <param name="command"></param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public static object Process(this Command command, IDataParameter[] parameters,
            IDbTransaction transaction = null)
        {
            IDbConnection connection = transaction != null
                ? transaction.Connection
                : Commands.DbConnection(command.DbName);

            IDbCommand dbCommand = connection.CreateCommand();
            dbCommand.CommandText = command.Text;
            dbCommand.CommandType = CommandType.StoredProcedure;
            foreach (IDataParameter dataParameter in parameters)
                dbCommand.Parameters.Add(dataParameter);

            try
            {
                if (transaction != null)
                    dbCommand.Transaction = transaction;
                else
                    connection.OpenIfClose();
                IDbDataParameter dbParameter = dbCommand.CreateParameter();
                dbParameter.ParameterName = "RetVal";
                dbParameter.Direction = ParameterDirection.ReturnValue;
                dbCommand.Parameters.Add(dbParameter);
                dbCommand.ExecuteNonQuery();
                return dbParameter.Value;
            }
            catch (Exception ex)
            {
                if (transaction != null)
                    transaction.Rollback();

                ex.Process();
                throw;
            }
            finally
            {
                if (transaction == null)
                    connection.CloseIfOpen();
            }
        }
    }
}