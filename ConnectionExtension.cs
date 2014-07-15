using System.Data;

namespace SqlPipe
{
    public static class ConnectionExtension
    {
        public static void CloseIfOpen(this IDbConnection connection)
        {
            if (connection.State == ConnectionState.Open ||
                connection.State == ConnectionState.Broken)
                connection.Close();
        }

        public static void OpenIfClose(this IDbConnection connection)
        {
            if (connection.State == ConnectionState.Broken)
                connection.Close();

            if (connection.State == ConnectionState.Closed)
                connection.Open();
        }
    }
}