using CsvHelper;
using SalaProconPegaNumeros;
using System;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Linq;
using System.Text.RegularExpressions;
using System.Net.Mail;
using System.Net;

class Program
{
    static async Task Main()
    {
        string connectionString = "Data Source=172.22.0.90;Initial Catalog=numerosProcon;User Id=kaiky;Password='xBU#3@p7';";
        string outputDirectory = @"\\172.22.0.82\ti\3 - Números - Procon\arquivoTransformado.txt";
        string outputDirectoryCSV = @"\\172.22.0.82\ti\3 - Números - Procon";

        try
        {
            TransferirDados(connectionString);
            Console.WriteLine("Dados transferidos com sucesso!");

            // Após transferir dados, baixar um arquivo
            string urlDoArquivo = "https://4110011procondynamics365.blob.core.windows.net/nml-csv/NumerosNaoMeLigue.csv?sv=2018-03-28&sr=b&sig=6sfVOsDr3XA4DaTTuxizpD47h35W4potk%2FsvC6uEVPo%3D&se=2033-06-26T14%3A50%3A52Z&sp=rl";
            string caminhoLocalDoArquivoCsv = Path.Combine(outputDirectoryCSV, "arquivoBaixado.csv");

            await BaixarArquivoAsync(urlDoArquivo, caminhoLocalDoArquivoCsv);
            Console.WriteLine("Arquivo baixado com sucesso!");

            // Transformar em TXT e renomear cabeçalhos
            string caminhoTransformado = TransformarEAlterarCabecalhos(caminhoLocalDoArquivoCsv);
            Console.WriteLine("Arquivo transformado e cabeçalhos alterados com sucesso: " + caminhoTransformado);

            // Remover aspas do arquivo de texto
            RemoverAspas(caminhoTransformado);
            Console.WriteLine("Aspas removidas do arquivo de texto.");

            string caminhoArquivoTxt = @"\\172.22.0.82\ti\3 - Números - Procon\arquivoTransformado.txt";

            RemoverVirgulasDosNumeros(caminhoArquivoTxt);
            Console.WriteLine("Vírgulas no meio dos números removidas do arquivo de texto.");

            // importar novos dados a tabela Numeros do Procon
            ImportarDadosParaTabela(connectionString, outputDirectory);
            Console.WriteLine($"Novos dados importados para tabela Numeros as {DateTime.Now}");

            EnviarEmailNotificacao(connectionString);
         }

        catch (Exception ex)
        {
            Console.WriteLine("Ocorreu um erro: " + ex.Message);
        }

        Console.ReadKey();

    }

    static void TransferirDados(string connectionString)
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            connection.Open();

            string sqlCommand = @"
            INSERT INTO NumerosDiscador (Telefone, DDD)
            SELECT DISTINCT Telefone, LEFT(Telefone, 2) AS DDD
            FROM Numeros
            WHERE Telefone IS NOT NULL;";

            using (SqlCommand command = new SqlCommand(sqlCommand, connection))
            {
                command.CommandTimeout = 0; 
                command.ExecuteNonQuery();
            }

            connection.Close();
        }
    }

    static async Task BaixarArquivoAsync(string url, string caminhoLocal)
    {
        using (HttpClient httpClient = new HttpClient())
        {
            HttpResponseMessage resposta = await httpClient.GetAsync(url);

            if (resposta.IsSuccessStatusCode)
            {
                using (var stream = await resposta.Content.ReadAsStreamAsync())
                using (var fileStream = File.Create(caminhoLocal))
                {
                    await stream.CopyToAsync(fileStream);
                    fileStream.Close();
                }
            }
            else
            {
                Console.WriteLine($"Falha ao baixar o arquivo. Código de status: {resposta.StatusCode}");
            }
        }
    }

    static string TransformarEAlterarCabecalhos(string caminhoArquivoCsv)
    {
        string caminhoTransformado = Path.Combine(Path.GetDirectoryName(caminhoArquivoCsv), "arquivoTransformado.txt");

        // Lê o arquivo CSV e escreve o conteúdo transformado no arquivo de texto
        using (var reader = new StreamReader(caminhoArquivoCsv))
        using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
        using (var writer = new StreamWriter(caminhoTransformado))
        {
            // Lê os registros do CSV
            csv.Read();
            csv.ReadHeader();

            // Obtém os cabeçalhos do CSV
            var cabecalhos = csv.HeaderRecord;

            // Escreve os cabeçalhos modificados no arquivo de texto
            writer.WriteLine(string.Join(",", cabecalhos.Select(CorrigirNomeCabecalho)));

            // Escreve os registros no arquivo de texto
            while (csv.Read())
            {
                writer.WriteLine($"{csv.GetField<string>("numero")},{csv.GetField<string>("DataCadastro")}");
            }
        }

        return caminhoTransformado;
    }

    static string CorrigirNomeCabecalho(string nomeOriginal)
    {
        // Substitui os nomes dos cabeçalhos
        switch (nomeOriginal)
        {
            case "numero":
                return "Telefone";
            case "DataCadastro":
                return "DataDeCadastro";
            // Adicione mais casos conforme necessário
            default:
                return nomeOriginal;
        }
    }

    static void RemoverAspas(string caminhoArquivoTxt)
    {
        // Lê o conteúdo do arquivo de texto
        string conteudo = File.ReadAllText(caminhoArquivoTxt);

        // Remove todas as aspas (simples e duplas)
        conteudo = conteudo.Replace("'", string.Empty).Replace("\"", string.Empty);

        // Escreve o conteúdo modificado de volta no arquivo de texto
        File.WriteAllText(caminhoArquivoTxt, conteudo);
    }

    static void RemoverVirgulasDosNumeros(string caminhoArquivoTxt)
    {
        try
        {
            // Lê o conteúdo do arquivo de texto
            string conteudo = File.ReadAllText(caminhoArquivoTxt);

            // Remove todas as vírgulas
            conteudo = conteudo.Replace(",", string.Empty);

            // Adiciona uma vírgula antes de cada data, se não houver vírgula na linha antes da data
            conteudo = Regex.Replace(conteudo, @"(?<!\d,)(\d{2}/\d{2}/\d{4})", @",$1");

            // Escreve o conteúdo modificado de volta no arquivo de texto
            File.WriteAllText(caminhoArquivoTxt, conteudo);

            Console.WriteLine("Operação concluída com sucesso.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ocorreu um erro: {ex.Message}");
        }
    }

    static void ImportarDadosParaTabela(string connectionString, string caminhoArquivoTxt)
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            connection.Open();

            // Comando SQL BULK INSERT
            string bulkInsertCommand = $@"
            BULK INSERT Numeros
            FROM '\\172.22.0.82\ti\3 - Números - Procon\arquivoTransformado.txt'
            WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2  -- Pule a primeira linha se ela contiver cabeçalhos
            );";

            using (SqlCommand command = new SqlCommand(bulkInsertCommand, connection))
            {
                command.CommandTimeout = 0; // 0 significa sem limite de tempo
                command.ExecuteNonQuery();
            }

            connection.Close();
        }
    }

    static void EnviarEmailNotificacao(string connectionString)
    {
        try
        {
            // Configurar as credenciais do e-mail
            var credenciais = new NetworkCredential("desenvolvimento@salasolutions.com.br", "#Sala123");

            // Configurar o cliente SMTP para o Zimbra
            using (SmtpClient clienteSmtp = new SmtpClient("mail.salasolutions.com.br"))
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    // Obtém a data do dia anterior
                    DateTime dataAlvo = DateTime.Now.Date.AddDays(-1);

                    connection.Open();

                    int totalNumerosAntesRemocao = 0;
                    int totalNumerosDepoisRemocao = 0;
                    int totalNumerosDiscador = 0;
                    int totalNumerosNovos = 0;

                    // Consultar a quantidade de registros na tabela Numeros antes da remoção
                    string queryNumerosAntesRemocao = "SELECT COUNT(*) Telefone FROM NumerosDiscador";
                    using (SqlCommand commandNumerosAntesRemocao = new SqlCommand(queryNumerosAntesRemocao, connection))
                    {
                        commandNumerosAntesRemocao.CommandTimeout = 0;
                        totalNumerosAntesRemocao = (int)commandNumerosAntesRemocao.ExecuteScalar();
                    }

                    // Remover os números da tabela NumerosDiscador ou qualquer outra tabela onde os números são removidos.

                    // Consultar a quantidade de registros na tabela Numeros depois da remoção
                    string queryNumerosDepoisRemocao = "SELECT COUNT(*) AS TotalNumeros FROM Numeros";
                    using (SqlCommand commandNumerosDepoisRemocao = new SqlCommand(queryNumerosDepoisRemocao, connection))
                    {
                        commandNumerosDepoisRemocao.CommandTimeout = 0;
                        totalNumerosDepoisRemocao = (int)commandNumerosDepoisRemocao.ExecuteScalar();
                    }

                    // Consultar a quantidade de registros na tabela NumerosDiscador
                    string queryNumerosDiscador = "SELECT COUNT(*) AS TotalNumerosDiscador FROM NumerosDiscador";
                    using (SqlCommand commandNumerosDiscador = new SqlCommand(queryNumerosDiscador, connection))
                    {
                        commandNumerosDiscador.CommandTimeout = 0;
                        totalNumerosDiscador = (int)commandNumerosDiscador.ExecuteScalar();
                    }

                    string queryNumerosNovos = "SELECT COUNT(*) FROM Numeros WHERE Telefone NOT IN (SELECT Telefone FROM NumerosDiscador)";
                    using (SqlCommand commandNumerosNovos = new SqlCommand(queryNumerosNovos, connection))
                    {
                        commandNumerosNovos.CommandTimeout = 0;
                        totalNumerosNovos = (int)commandNumerosNovos.ExecuteScalar();
                    }

                    int totalPesquisaNumeros;
                    int totalNumeroPesquisandos;
                    int geralDeNumerosPesquisados;

                    string queryPesquisaNumeros = "SELECT COUNT(*) FROM PesquisaNumeros WHERE Detalhes >= @DataInicio AND Detalhes < @DataFim";
                    using (SqlCommand commandPesquisaNumeros = new SqlCommand(queryPesquisaNumeros, connection))
                    {
                        commandPesquisaNumeros.CommandTimeout = 0;
                        commandPesquisaNumeros.Parameters.AddWithValue("@DataInicio", dataAlvo);
                        commandPesquisaNumeros.Parameters.AddWithValue("@DataFim", dataAlvo.AddDays(1));
                        totalPesquisaNumeros = (int)commandPesquisaNumeros.ExecuteScalar();
                    }

                    string queryNumeroPesquisandos = "SELECT COUNT(*) FROM NumeroPesquisandos WHERE Detalhes >= @DataInicio AND Detalhes < @DataFim";
                    using (SqlCommand commandNumeroPesquisandos = new SqlCommand(queryNumeroPesquisandos, connection))
                    {
                        commandNumeroPesquisandos.CommandTimeout = 0;
                        commandNumeroPesquisandos.Parameters.AddWithValue("@DataInicio", dataAlvo);
                        commandNumeroPesquisandos.Parameters.AddWithValue("@DataFim", dataAlvo.AddDays(1));
                        totalNumeroPesquisandos = (int)commandNumeroPesquisandos.ExecuteScalar();

                        Console.WriteLine($"Número de pesquisas na tabela 'NumeroPesquisandos' no dia {dataAlvo}: {totalNumeroPesquisandos}");
                    }


                    string queryGeralPesquisas = "SELECT (SELECT COUNT(*) FROM NumeroPesquisandos) + (SELECT COUNT(*) FROM PesquisaNumeros);";
                    using (SqlCommand totalPesquisados = new SqlCommand(queryGeralPesquisas, connection))
                    {
                        geralDeNumerosPesquisados = (int)totalPesquisados.ExecuteScalar();
                        connection.Close();
                    }


                    // Calcular a diferença entre as quantidades
                    int diferencaEntradaSaida = totalNumerosDepoisRemocao - totalNumerosAntesRemocao;

                    clienteSmtp.UseDefaultCredentials = false;
                    clienteSmtp.Credentials = credenciais;
                    clienteSmtp.Port = 587;
                    clienteSmtp.EnableSsl = false;

                    // Criar a mensagem de e-mail
                    MailMessage mensagem = new MailMessage();
                    mensagem.From = new MailAddress("desenvolvimento@salasolutions.com.br");

                    mensagem.To.Add("estrategia01@salasolutions.com.br");

                    mensagem.Subject = "Atualização Procon";
                    mensagem.Body = $"Usuários, os números do Procon foram atualizados com sucesso na Tabela 'Numeros' no Banco de Dados 'nomerosProcon'." +
                        $"\n" +
                        $"\nHorário e data da Atualização: {DateTime.Now}" +
                        $"\nQuantidade de Numeros na base antes da remoção: {totalNumerosAntesRemocao}" +
                        $"\nQuantidade de Numeros na base depois da remoção: {totalNumerosDepoisRemocao}" +
                        $"\nNumeros que entraram: {diferencaEntradaSaida}" +
                        $"\nNúmeros cadastrados no Procon que foram pesquisados: {totalPesquisaNumeros}" +
                        $"\nNúmeros pesquisados, mas que não estão na base do procon: {totalNumeroPesquisandos}" +
                        $"\nGeral de números pesquisados: {geralDeNumerosPesquisados} desde o dia 06/11/2023";

                    // Enviar o e-mail
                    clienteSmtp.Send(mensagem);

                    connection.Close();
                }
            }
            Console.WriteLine("E-mail enviado com sucesso.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Erro ao enviar o e-mail: " + ex.Message);
        }
    }
}