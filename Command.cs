using System;
using System.Data;
using System.Xml.Serialization;

namespace SqlPipe
{
    /// <summary>
    ///     Sql 命令
    /// </summary>
    [Serializable]
    public class Command
    {
        private CommandType _commandType;

        public Command()
        {
            _commandType = CommandType.Text;
        }

        /// <summary>
        ///     预编译
        /// </summary>
        public bool Precompiled { get; set; }

        /// <summary>
        ///     ID
        /// </summary>
        [XmlAttribute("id")]
        public string Id { get; set; }

        /// <summary>
        ///     DataBase Name
        /// </summary>
        [XmlAttribute("dbName")]
        public string DbName { get; set; }

        /// <summary>
        ///     Command Text
        /// </summary>
        public string Text { get; set; }

        /// <summary>
        ///     Command Type
        /// </summary>
        [XmlAttribute("commandType")]
        public CommandType CommandType
        {
            get { return _commandType; }
            set { _commandType = value; }
        }
    }
}