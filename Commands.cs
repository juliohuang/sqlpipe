using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Xml.Serialization;

namespace SqlPipe
{
    /// <summary>
    ///     Commands
    /// </summary>
    [Serializable]
    [XmlRoot("Commands")]
    public class Commands : List<Command>
    {
        private static Commands _commands;

        private static volatile Type _defaultConncetionType = typeof (SqlConnection);

        private static string _defaultDbName = "main";

        public static Type DefaultConncetionType
        {
            get { return _defaultConncetionType; }
            set { _defaultConncetionType = value; }
        }

        public static string DefaultDbName
        {
            get { return _defaultDbName; }
            set { _defaultDbName = value; }
        }
        public static Hashtable c=new Hashtable();
        public static Command GetCommand(string sql, string dbName = null, bool precompiled = false)
        {
            if (c.Contains(sql))
            {
                return (Command)c[sql];
            }
            var command = new Command {DbName = dbName ?? DefaultDbName, Text = sql, Precompiled = precompiled};
            c.Add(sql,command);
            return command;
        }

        public static IDbTransaction BeginTransaction(string dbName = null)
        {
            IDbConnection connection = DbConnection(dbName ?? DefaultDbName);
            connection.OpenIfClose();
            return connection.BeginTransaction();
        }

        public static IDbConnection DbConnection(string name)
        {
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings[name];

            // default SqlServer
            string providerName = settings.ProviderName;
            string connectionString = settings.ConnectionString;

            Type type = String.IsNullOrEmpty(providerName) ? DefaultConncetionType : Type.GetType(providerName);
            Debug.Assert(type != null, "type != null");

            object instance = Activator.CreateInstance(type, connectionString);
            return instance as IDbConnection;
        }

        public static T Procedure<T>()
        {
            return default(T);
        }
    }
}