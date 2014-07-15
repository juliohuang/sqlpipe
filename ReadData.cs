using System.Data;

namespace SqlPipe
{
    public delegate void ReadData(IDataReader reader);

    public delegate void ReadData<in T>(IDataReader reader, T t);
}