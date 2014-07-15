using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace SqlPipe
{
    public partial class CommandExtension
    {
        /// <summary>
        ///     执行
        /// </summary>
        /// <param name="command"></param>
        /// <param name="paras"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public static bool Exec(this Command command, object paras, IDbTransaction transaction = null)
        {
            Dictionary<string, object> dictionary = ToDictionary(command, paras);
            return Exec(command, dictionary, transaction);
        }


        /// <summary>
        ///     查询
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <param name="paras"></param>
        /// <param name="transaction"></param>
        /// <returns></returns>
        public static T Read<T>(this Command command, object paras, IDbTransaction transaction = null)
        {
            Dictionary<string, object> objects = ToDictionary(command, paras);

            var instance = Activator.CreateInstance<T>();
            var result = instance as IDictionary;
            if (result != null)
            {
                ReadDictionary(command, result, objects, transaction);
                return instance;
            }

            return default(T);
        }
    }
}