using System.Data;

namespace SqlPipe
{
    public delegate void CommandProcess(IDbCommand command);

    public delegate T CommandProcess<T>(IDbCommand command,T result);
}