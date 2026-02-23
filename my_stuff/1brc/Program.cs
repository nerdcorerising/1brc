using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace _1brc;

static class Constants
{
    public const int BufferSize = 131072;
}

class WeatherStationStats
{
    public double Min;
    public double Max;
    public double Avg;
    // Processed count, does not include those in _unprocessed
    public int Count;

    
    public WeatherStationStats()
    {
        Min = Double.MaxValue;
        Max = Double.MinValue;
        Avg = 0;
        Count = 0;
    }

    public void AddMeasurement(double measurement)
    {
        if (measurement > Max)
        {
            Max = measurement;
        }

        if (measurement < Min)
        {
            Min = measurement;
        }

        ++Count;
        double newAvg = (measurement * ((double)1 / Count)) + (Avg * ((double)(Count - 1) / Count));
        Avg = newAvg;
    }
}

class Program
{   
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    static void Main(string[] args)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        ConcurrentQueue<Tuple<char[], int>> availableBuffers = new ConcurrentQueue<Tuple<char[], int>>();
        CancellationTokenSource cts = new CancellationTokenSource();
        // List<Thread> threads = new List<Thread>();
        // TODO, not thread safe but only using one thread right now
        Dictionary<string, WeatherStationStats> stats = new Dictionary<string, WeatherStationStats>();

        Thread t = new Thread(() => ProcessThread(availableBuffers, cts.Token, stats));
        t.Start();
        
        int count = 0;
        using (StreamReader input = new StreamReader(new FileStream(@"C:\git\1brc\my_stuff\1brc\measurements\measurements_100m.txt", FileMode.Open, FileAccess.Read)))
        {
            while (true)
            {
                char[] buffer = GetBuffer();
                int read = input.Read(buffer, 0, buffer.Length);
                if (read <= 0)
                {
                    break;
                }
                Tuple<char[], int> readBuffer = Tuple.Create(buffer, read);
                availableBuffers.Enqueue(readBuffer);
            }
        }
        cts.Cancel();
        t.Join();
        
        Console.Write('{');
        var sortedList = stats.Keys.ToList();
        sortedList.Sort();
        for (int i = 0; i < sortedList.Count; ++i)
        {
            string name = sortedList[i];
            Console.Write($"{name}={stats[name].Min:0.0}/{stats[name].Avg:0.0}/{stats[name].Max:0.0}");
            if (i  < stats.Count - 1)
            {
                Console.Write(", ");
            }
        }

        Console.WriteLine('}');
        Console.WriteLine($"Calculated {count:N0} items in {stopwatch.Elapsed}");
    }
    
    private static void ProcessThread(ConcurrentQueue<Tuple<char[], int>> availableBuffers, CancellationToken ct, Dictionary<string, WeatherStationStats> stats)
        {
            bool partialRead = false;
            char[] partialReadBuffer = GetBuffer();
            int partialReadBufferPos = 0;
            var readOnlySpanLookup = stats.GetAlternateLookup<ReadOnlySpan<char>>();
            while (true)
            {
                Tuple<char[], int> readBuffer;
                if (!availableBuffers.TryDequeue(out readBuffer))
                {
                    if (ct.IsCancellationRequested)
                    {
                        break;
                    }

                    Thread.Sleep(10);
                    continue;
                }

                char[] buffer = readBuffer.Item1;
                int bufferSize = readBuffer.Item2;

                int pos = 0;
                if (partialRead)
                {
                    while (pos < bufferSize && buffer[pos] != '\n')
                    {
                        partialReadBuffer[partialReadBufferPos++] = buffer[pos++];
                    }
                    if (pos >= bufferSize) throw new InvalidDataException(); // Shouldn't see this
                    
                    partialReadBuffer[partialReadBufferPos] = '\n';
                    ++pos;

                    // Ugh, duplicate code from below
                    ReadOnlySpan<char> name;
                    double value;
                    GetNameAndValueFromBuffer(partialReadBuffer, 0, out name, out value);

                    if (!readOnlySpanLookup.ContainsKey(name))
                    {
                        stats.Add(name.ToString(), new WeatherStationStats());
                    }

                    readOnlySpanLookup[name].AddMeasurement(value);

                    partialRead = false;
                    partialReadBufferPos = 0;
                }

                while (true)
                {
                    int startPos = pos;
                    while (pos < bufferSize && buffer[pos] != '\n')
                    {
                        ++pos;
                    }

                    ++pos;

                    bool shouldBreak = false;
                    ReadOnlySpan<char> name;
                    double value;
                    if (pos < bufferSize)
                    {
                        // great, it's the normal case and we can just read from the buffer
                        GetNameAndValueFromBuffer(buffer, startPos, out name, out value);
                    }
                    else
                    {
                        // We're at the end of the buffer
                        if (pos == bufferSize && buffer[pos - 1] == '\n')
                        {
                            // Can still read from the buffer but need a new buffer after
                            GetNameAndValueFromBuffer(buffer, startPos, out name, out value);
                            shouldBreak = true;
                        }
                        else
                        {
                            // Now we're in the partial read case. Copy the partial to the partial buffer
                            partialReadBufferPos = bufferSize - startPos;
                            Array.Copy(buffer, startPos, partialReadBuffer, 0, partialReadBufferPos);
                            partialRead = true;
                            break;
                        }
                    }

                    if (!readOnlySpanLookup.ContainsKey(name))
                    {
                        stats.Add(name.ToString(), new WeatherStationStats());
                    }

                    readOnlySpanLookup[name].AddMeasurement(value);

                    if (shouldBreak)
                    {
                        ReturnBuffer(buffer);
                        break;
                    }
                }
            }
        }

    private static void GetNameAndValueFromBuffer(char[] buffer, int startPos, out ReadOnlySpan<char> name, out double value)
    {
        //  TODO: This will go over every character twice, once above looking for \n and again here looking for ; and \n 
        int pos = startPos;
        while (pos < buffer.Length && buffer[pos] != ';')
        {
            ++pos;
        }
        if (pos >= buffer.Length) throw new InvalidDataException();

        name = new ReadOnlySpan<char>(buffer, startPos, pos - startPos);

        // Skip the ;
        ++pos;
        startPos = pos;
        while (pos < buffer.Length && buffer[pos] != '\n')
        {
            ++pos;
        }
        if (pos >= buffer.Length) throw new InvalidDataException();
        
        value = Double.Parse(new ReadOnlySpan<char>(buffer, startPos, pos - startPos));
    }

    private static char[] GetBuffer()
    {
        return ArrayPool<char>.Shared.Rent(Constants.BufferSize);
    }
    
    private static void ReturnBuffer(char[] buffer)
    {
        ArrayPool<char>.Shared.Return(buffer);
    }
}