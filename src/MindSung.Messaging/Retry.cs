using System;
using System.Threading.Tasks;

namespace MindSung
{
    public static class Retry
    {
        public static T Do<T>(Func<T> action, Action beforeRetry, int retries = 2)
        {
            while (true)
            {
                try { return action(); }
                catch
                {
                    if (retries-- <= 0) throw;
                    if (beforeRetry != null)
                    {
                        try { beforeRetry(); }
                        catch { }
                    }
                }
            }
        }

        public static T Do<T>(Func<T> action, int retries = 2)
        {
            return Do(action, null, retries);
        }

        public static void Do(Action action, Action beforeRetry, int retries = 2)
        {
            Do(() => { action(); return true; }, beforeRetry, retries);
        }

        public static void Do(Action action, int retries = 2)
        {
            Do(() => { action(); return true; }, null, retries);
        }

        public static async Task<T> DoAsync<T>(Func<Task<T>> action, Action beforeRetry, int delayMsBeforeRetry = 0, int retries = 2)
        {
            while (true)
            {
                try { return await action(); }
                catch { if (retries-- <= 0) throw; }
                if (delayMsBeforeRetry > 0) await Task.Delay(delayMsBeforeRetry);
                if (beforeRetry != null)
                {
                    try { beforeRetry(); }
                    catch { }
                }
            }
        }

        public static Task<T> DoAsync<T>(Func<Task<T>> action, int delayMsBeforeRetry = 0, int retries = 2)
        {
            return DoAsync(action, null, delayMsBeforeRetry, retries);
        }

        public static Task DoAsync(Action action, Action beforeRetry, int delayMsBeforeRetry = 0, int retries = 2)
        {
            return DoAsync(() => { action(); return Task.FromResult(true); }, beforeRetry, delayMsBeforeRetry, retries);
        }

        public static Task DoAsync(Action action, int delayMsBeforeRetry = 0, int retries = 2)
        {
            return DoAsync(action, null, delayMsBeforeRetry, retries);
        }

        public static Task DoAsync(Func<Task> action, Action beforeRetry, int delayMsBeforeRetry, int retries = 2)
        {
            return DoAsync(async () => { await action(); return Task.FromResult(true); }, beforeRetry, delayMsBeforeRetry, retries);
        }

        public static Task DoAsync(Func<Task> action, int delayMsBeforeRetry, int retries = 2)
        {
            return DoAsync(action, null, delayMsBeforeRetry, retries);
        }
    }
}
