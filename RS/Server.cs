// МНОГОПОТОЧНЫЙ TCP - СЕРВЕР прослушивает (в бесконечном цикле) все входящие подключения по порту
using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Npgsql;

namespace AppTcpServer
{
    public class ClientObject //отдельное подключение
    {
        private static List<TcpClient> _clients = new List<TcpClient>();
        public TcpClient client;
        private string username;
        private readonly Logger<ClientObject> logger;

        private const string connString = "Host=sersh.keenetic.name;Port=5432;Username=postgres;Password=Stnkr911;Database=RS";

        public ClientObject(TcpClient tcpClient, Logger<ClientObject> logger)
        {
            this.client = tcpClient;
            this.logger = logger;

            lock (_clients)
            {
                _clients.Add(client);
            }   
        }

        public void Process()
        {
            NetworkStream stream = null;
            
            try
            {
                logger.LogInformation("Началась обработка пользователя.");

                stream = client.GetStream();
                byte[] buffer = new byte[1024];
                int bytes = 0;
                StringBuilder stringBuilder = new StringBuilder();
                do
                {
                    bytes = stream.Read(buffer, 0, buffer.Length);
                    stringBuilder.Append(Encoding.UTF8.GetString(buffer, 0, bytes));
                }
                while (stream.DataAvailable);

                username = stringBuilder.ToString();
                logger.LogInformation("Подключился пользователь: " + username);

                if (!CheckUserOrRequestNewUsername(ref username))
                {
                    SendMessageToClient(stream, "INVALID_USERNAME");

                    return;
                }

                SendChatHistoryToClient(stream);

                byte[] data = new byte[1024]; // буфер для получаемых данных
                while (true)
                {
                    logger.LogInformation("Началась обработка сообщений пользователя.");

                    // получаем сообщение в цикле  и потом немного его изменяем 
                    // отрезаем по двоеточию и отправляем обратно клиенту
                    StringBuilder builder = new StringBuilder();
                    bytes = 0;
                    do
                    {
                        bytes = stream.Read(data, 0, data.Length);
                        builder.Append(Encoding.UTF8.GetString(data, 0, bytes));
                    }
                    while (stream.DataAvailable);

                    string message = builder.ToString();
                    logger.LogInformation(message);

                    DateTime sendingTime = DateTime.UtcNow;
                    InsertIntoData(username, message, sendingTime);

                    BroadcastMessage(message);
                }
            }
            catch (Exception ex)
            {
                logger.LogError("Ошибка при обработке сообщений: " + ex.Message);
            }
            finally
            {
                if (stream != null)
                    stream.Close();
                if (client != null)
                    client.Close();

                logger.LogInformation("Обработка пользователя закончена.");
            }
        }

        private void SendMessageToClient(NetworkStream stream, string message)
        {
            byte[] data = Encoding.UTF8.GetBytes(message);
            stream.Write(data, 0, data.Length);
        }

        private void BroadcastMessage(string message)
        {
            lock(_clients)
            {
                byte[] data = Encoding.UTF8.GetBytes(message);
                foreach (var client in _clients)
                {
                    try
                    {
                        client.GetStream().Write(data, 0, data.Length);
                    }
                    catch
                    {
                        // Если клиент отключился — удаляем его
                        client.Close();
                        logger.LogInformation("Обработка пользователя закончена.");
                    }
                }
            }
        }

        private void SendChatHistoryToClient(NetworkStream stream)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(connString))
            {
                connection.Open();
                using (var cmd = new NpgsqlCommand("SELECT username, message, sending_time FROM data ORDER BY sending_time ASC LIMIT 100", connection))
                using (var reader = cmd.ExecuteReader())
                {
                    StringBuilder result = new StringBuilder();
                    while (reader.Read())
                    {
                        string user = reader.GetString(0);
                        string msg = reader.GetString(1);
                        result.AppendLine($"{msg}");
                    }
                    byte[] data = Encoding.UTF8.GetBytes(result.ToString());
                    stream.Write(data, 0, data.Length);
                }
            }
        }

        private void InsertIntoData(string username, string message, DateTime sendingTime)
        {
            try
            {
                using (NpgsqlConnection connection = new NpgsqlConnection(connString))
                {
                    connection.Open();
                    using (var cmd = new NpgsqlCommand("INSERT INTO data (message, username, sending_time) VALUES (@message, @username, @sending_time)", connection))
                    {
                        cmd.Parameters.AddWithValue("message", message);
                        cmd.Parameters.AddWithValue("username", username);
                        cmd.Parameters.AddWithValue("sending_time", sendingTime);
                        cmd.ExecuteNonQuery();
                    }
                }

                logger.LogInformation("Сообщение сохранено.");
            }
            catch (Exception ex)
            {
                logger.LogError("Ошибка при сохранении сообщения: " +  ex.Message);
            }
        }

        private bool CheckUserOrRequestNewUsername(ref string username)
        {
            try
            {
                using (NpgsqlConnection connection = new NpgsqlConnection(connString))
                {
                    connection.Open();
                    using (var cmd = new NpgsqlCommand("SELECT * FROM users WHERE username = @username", connection))
                    {
                        cmd.Parameters.AddWithValue("username", username);
                        object result = cmd.ExecuteScalar();

                        if (result != null)
                        {
                            return true;
                        }
                        else
                        {
                            using (var insertCmd = new NpgsqlCommand("INSERT INTO users (username) VALUES (@username)", connection))
                            {
                                insertCmd.Parameters.AddWithValue("username", username);
                                insertCmd.ExecuteNonQuery();
                            }
                            return true;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }
    }


    class Program
    {
        const int port = 8080;
        static TcpListener listener;

        static void Main(string[] args)
        {
            using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            Logger<ClientObject> logger = new Logger<ClientObject>(loggerFactory);

            try
            {
                listener = new TcpListener(IPAddress.Parse("0.0.0.0"), port);
                listener.Start();

                Console.WriteLine("Ожидание подключений...");

                while (true)
                {
                    TcpClient client = listener.AcceptTcpClient(); // подключение нового клиента.
                    ClientObject clientObject = new ClientObject(client, logger);

                    // создаем новый поток для обслуживания нового клиента
                    Thread clientThread = new Thread(new ThreadStart(clientObject.Process));
                    clientThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                if (listener != null)
                    listener.Stop();
            }
        }
    }
}