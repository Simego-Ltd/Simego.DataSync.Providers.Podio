using System;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Simego.DataSync.Providers.Podio.TypeEditors
{
    [System.Diagnostics.DebuggerStepThrough]
    class WebServer
    {
        private readonly HttpListener _webListener;
        private volatile bool _run = true;
        
        public Func<HttpListenerContext, WebMessage> PageRequestCallback;
        public Action<WebMessage> PageResponseCompleted = null;

        public void Stop()
        {
            _run = false;
            if (_webListener != null)
            {
                _webListener.Stop();
            }
        }

        public WebServer(int port)
        {
            try
            {
                if (!HttpListener.IsSupported)
                    throw new NotSupportedException("WebServer not supported on this platform.");

                //start listing on the given port
                _webListener = new HttpListener
                {
                    AuthenticationSchemes = AuthenticationSchemes.Anonymous,
                    UnsafeConnectionNtlmAuthentication = true,
                    IgnoreWriteExceptions = true                    
                };

                _webListener.Prefixes.Add(string.Format("http://localhost:{0}/", port));
                
                //Start the Listener
                _webListener.Start();

                //Start the thread which waits for inbound connections.
                Thread th = new Thread(WaitForConnection) { IsBackground = true };
                th.Start();

            }
            catch (HttpListenerException httpListenerException)
            {
                if (httpListenerException.Message.Equals("Access is Denied", StringComparison.OrdinalIgnoreCase))
                {
                    throw new ApplicationException(
                        string.Format("Unable to Start the HTTP Listener on this address 'http:/{0}:{1}/' please run the following windows command to reserve the address.\n\nnetsh http add urlacl url=http:/{0}:{1}/ user={2}\\{3}",
                        Environment.MachineName,
                        port,
                        Environment.UserDomainName,
                        Environment.UserName), httpListenerException);
                }

            }
        }

        public void WaitForConnection()
        {            
            while (_run)
            {
                //Accept a new connection
                try
                {
                    //Start the Listener
                    _webListener.Start();
                    //Blocks until we get an inbound connection.
                    HttpListenerContext context = _webListener.GetContext();
                    Task.Run(() => HandleRequest(context));
                }
                catch (HttpListenerException)
                {

                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (ThreadAbortException)
                {
                    break;
                }
            }

            if (_webListener != null)
                _webListener.Stop();
        }

        private void HandleRequest(HttpListenerContext context)
        {
            if (context == null)
                return;

            HttpListenerResponse response = context.Response;

            try
            {
                //Notify the Client about the Message
                if (PageRequestCallback != null)
                {
                    WebMessage webMessage = PageRequestCallback(context);

                    //If we have a message to return then send it.
                    if (webMessage != null)
                        webMessage.Send(response);

                    //Send the Completed Message
                    PageResponseCompleted?.Invoke(webMessage);
                }
            }
            finally
            {
                response.Close();
            }
        }
    }

    class WebMessage
    {
        public string Message { get; set; }
        public string ContentType { get; set; }
        public int StatusCode { get; set; }

        public WebMessage()
        {
            ContentType = "text/html; charset=utf-8";
            StatusCode = 200;
        }

        public WebMessage(string message)
            : this()
        {
            Message = message;
        }

        public void Send(HttpListenerResponse response)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(Message);

            response.ContentType = ContentType;
            response.ContentLength64 = buffer.Length;
            response.StatusCode = StatusCode;

            if (response.OutputStream.CanWrite)
                response.OutputStream.Write(buffer, 0, buffer.Length);
        }
    }
}
