using System.Data;

namespace SqlPipe
{
    public delegate T PreRead<out T>(DataTable reader);
}