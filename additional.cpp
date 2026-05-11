#include <iostream>
#include <fstream>
#include <thread>
#include <queue>
#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <semaphore>
#include <iomanip>
#include <chrono>
#include <termios.h>
#include <unistd.h>
#include <fcntl.h>
#include <atomic>
#include <filesystem>
#include <sstream>
#include <cstdlib>
using namespace std;

#define CHUNK_SIZE 10
#define NUM_THREADS 4

// ================= STRUCTS =================
struct Chunk {
    int chunkID;
    string data;
    int bitCount;
    vector<unsigned char> bitString;
};

struct HuffmanNode {
    char ch;
    int freq;
    HuffmanNode *left, *right;
    HuffmanNode(char c, int f) {
        ch = c; freq = f;
        left = right = NULL;
    }
};

struct Compare {
    bool operator()(HuffmanNode* a, HuffmanNode* b) {
        return a->freq > b->freq;
    }
};

// ================= GLOBAL =================
vector<Chunk> comp_chunks;
long comp_fileSize;
int comp_numChunks;

priority_queue<pair<int,int>> comp_taskQueue;
mutex comp_mtx;
mutex writeMutex;

int BUFFER_SIZE = 4;

counting_semaphore<> comp_emptySlots(BUFFER_SIZE);
counting_semaphore<> comp_filledSlots(0);
bool comp_doneProducing = false;

// decompression globals
vector<string> decomp_chunks;
queue<int> decomp_taskQueue;
mutex decomp_mtx;
counting_semaphore<> decomp_emptySlots(BUFFER_SIZE);
counting_semaphore<> decomp_filledSlots(0);
bool decomp_doneProducing = false;

int decomp_freq[256];
vector<unsigned char> decomp_data;
vector<int> decomp_chunkSizes;
int decomp_numChunks;
int decomp_totalBits;

atomic<int> processedChunks(0);
atomic<long> currentCompressedSize(0);
atomic<long long> compressedBytes(0);
atomic<int> activeThreads(0);
string threadStatus[NUM_THREADS];
mutex screenMutex;
#define GREEN  "\033[32m"
#define RED    "\033[31m"
#define YELLOW "\033[33m"
#define CYAN   "\033[36m"
#define RESET  "\033[0m"
//===============File Checks==================
bool fileExists(const string& path) {
    ifstream file(path, ios::binary);
    return file.good();
}

bool hasBinExtension(const string& path) {
    if (path.size() < 4) return false;

    return path.substr(path.size() - 4) == ".bin";
}

string generateCompressedPath(const string& inputPath) {

    size_t slash = inputPath.find_last_of("/\\");

    string directory = "";
    string filename = inputPath;

    if (slash != string::npos) {
        directory = inputPath.substr(0, slash + 1);
        filename = inputPath.substr(slash + 1);
    }

    return directory + filename + ".bin";
}

string generateDecompressedPath(const string& inputPath) {

    size_t slash = inputPath.find_last_of("/\\");

    string directory = "";
    string filename = inputPath;

    if (slash != string::npos) {
        directory = inputPath.substr(0, slash + 1);
        filename = inputPath.substr(slash + 1);
    }

    if (filename.size() >= 4 &&
        filename.substr(filename.size() - 4) == ".bin") {

        filename = filename.substr(0, filename.size() - 4);
    }

    return directory + "decompressed_" + filename;
}
//==================TERMINAL UI====================
// ================= PROGRESS =================
void clearScreen() {
#ifdef _WIN32
    system("cls");
#else
    system("clear");
#endif
}
void progressBar(int total) {

    auto start = chrono::high_resolution_clock::now();

    while (processedChunks < total) {

        lock_guard<mutex> screenLock(screenMutex);

        clearScreen();

        int done = processedChunks;
        int percent = (done * 100) / total;

        auto now = chrono::high_resolution_clock::now();

        double elapsed =
        chrono::duration<double>(now - start).count();

        double speed =
        (elapsed > 0) ? done / elapsed : 0;

        double remaining =
        (speed > 0) ? (total - done) / speed : 0;

        double ratio =
        100.0 *
        (1.0 -
        ((double)currentCompressedSize /
         (double)comp_fileSize));

        cout << CYAN;
        cout << "====================================================\n";
        cout << "        MULTITHREADED FILE COMPRESSOR\n";
        cout << "====================================================\n";
        cout << RESET;

        cout << GREEN
             << "Processed Chunks : "
             << done << "/" << total << RESET << endl;

        cout << YELLOW
             << "Compressed Size  : "
             << currentCompressedSize / 1024.0
             << " KB" << RESET << endl;

        cout << GREEN
             << "Compression Ratio: "
             << fixed << setprecision(2)
             << ratio << "%" << RESET << endl;

        cout << CYAN
             << "Speed            : "
             << speed << " chunks/sec"
             << RESET << endl;

        cout << YELLOW
             << "ETA              : "
             << remaining << " sec"
             << RESET << endl;

        cout << "\nProgress:\n[";

        int bars = percent / 2;

        for (int i = 0; i < 50; i++) {

            if (i < bars)
                cout << "#";
            else
                cout << " ";
        }

        cout << "] " << percent << "%\n";

        cout << "\nThread Activity:\n";

        for (int i = 0; i < NUM_THREADS; i++) {

            cout << "Thread "
                 << i + 1
                 << " : "
                 << threadStatus[i]
                 << endl;
        }
        this_thread::sleep_for(
            chrono::milliseconds(100));
    }

    clearScreen();

    cout << GREEN;
    cout << "====================================================\n";
    cout << "              OPERATION COMPLETE\n";
    cout << "====================================================\n";
    sleep(2);
    cout << RESET;
}
// ================= HUFFMAN COMPRESSION ALGORITHM =================
HuffmanNode* buildTree(int freq[]) {
    priority_queue<HuffmanNode*, vector<HuffmanNode*>, Compare> pq;

    for (int i = 0; i < 256; i++)
        if (freq[i] > 0)
            pq.push(new HuffmanNode((char)i, freq[i]));

    if (pq.empty()) return NULL;

    while (pq.size() > 1) {
        auto l = pq.top(); pq.pop();
        auto r = pq.top(); pq.pop();
        auto m = new HuffmanNode('\0', l->freq + r->freq);
        m->left = l;
        m->right = r;
        pq.push(m);
    }
    return pq.top();
}

void generateCodes(HuffmanNode* root, string code, unordered_map<char,string>& codes) {
    if (!root) return;

    if (!root->left && !root->right) {
        if (code == "") code = "0";
        codes[root->ch] = code;
    }

    generateCodes(root->left, code + "0", codes);
    generateCodes(root->right, code + "1", codes);
}

// ================= FILE READ =================
void readChunkWorker(const string& filename, int index) {
    ifstream file(filename, ios::binary);
    int start = index * CHUNK_SIZE;
    file.seekg(start);
    int size = min(CHUNK_SIZE, (int)(comp_fileSize - start));
    string data(size, '\0');
    file.read(&data[0], size);

    lock_guard<mutex> lock(comp_mtx);   // ADD THIS
    comp_chunks[index].chunkID = index;
    comp_chunks[index].data = data;
}
bool loadFile(const string& filename) {
    ifstream file(filename, ios::binary);
    if (!file) return false;

    file.seekg(0, ios::end);
    comp_fileSize = file.tellg();
    if (comp_fileSize <= 0) {
    cout << "File is empty\n";
    return false;
}
    file.close();

    comp_numChunks = (comp_fileSize + CHUNK_SIZE - 1) / CHUNK_SIZE;
    comp_chunks.resize(comp_numChunks);

    for (int i = 0; i < comp_numChunks; i++)
    readChunkWorker(filename, i);

    return true;
}
// ================= COMPRESSION =================
vector<unsigned char> compressChunk(const string& data,
                                    unordered_map<char,string>& codes,
                                    int& bitCount) {
    vector<unsigned char> result;
    unsigned char buffer = 0;
    int bits = 0;
    bitCount = 0;

    for (char c : data) {
        string code = codes[c];
        bitCount += code.size();

        for (char b : code) {
            buffer <<= 1;
            if (b == '1') buffer |= 1;

            bits++;
            if (bits == 8) {
                result.push_back(buffer);
                buffer = 0;
                bits = 0;
            }
        }
    }

    if (bits > 0) {
        buffer <<= (8 - bits);
        result.push_back(buffer);
    }

    return result;
}

// ================= WORKER =================
void worker(unordered_map<char,string>& codes,
            int threadID){
    while (true) {
        
        comp_filledSlots.acquire();

        unique_lock<mutex> lock(comp_mtx);

        if (comp_taskQueue.empty() && comp_doneProducing)
            break;

        if (comp_taskQueue.empty())
            continue;

        int index = comp_taskQueue.top().second;
        threadStatus[threadID] =
"Compressing chunk " + to_string(index);
        comp_taskQueue.pop();

        lock.unlock();
        comp_emptySlots.release();
activeThreads++;

comp_chunks[index].bitString =
    compressChunk(comp_chunks[index].data,
                  codes,
                  comp_chunks[index].bitCount);

compressedBytes += comp_chunks[index].bitString.size();

processedChunks++;

activeThreads--;
    }
}

// ================= SAVE =================
void saveFile(const string& path,
              int* freq,
              vector<int>& sizes,
              vector<unsigned char>& data,
              int totalBits)
{
    ofstream out(path, ios::binary);

    out.write((char*)freq, 256*sizeof(int));
    out.write((char*)&comp_numChunks, sizeof(int));
    out.write((char*)sizes.data(), sizes.size()*sizeof(int));
    out.write((char*)&totalBits, sizeof(int));

    lock_guard<mutex> lock(writeMutex);
    out.write((char*)data.data(), data.size());

    out.close();
}

// ================= RUN COMPRESSION =================
void runCompression() {
    comp_chunks.clear();
comp_taskQueue = priority_queue<pair<int,int>>();
comp_doneProducing = false;
compressedBytes = 0;
currentCompressedSize = 0;
    processedChunks = 0;
    compressedBytes = 0;
activeThreads = 0;
    string in, out;

cout << "Enter input file: ";
getline(cin >> ws, in);

if (!fileExists(in)) {
    cout << "Input file does not exist\n";
    return;
}

cout << "Enter output file (.bin) [Press Enter for default]: ";
getline(cin, out);

if (out.empty()) {
    out = generateCompressedPath(in);
    cout << "Auto generated output: " << out << endl;
    this_thread::sleep_for(chrono::seconds(10));
}

if (!hasBinExtension(out)) {
    cout << "Compressed file must have .bin extension\n";
    return;
}
    if (!loadFile(in)) {
        cout << "Error loading file\n";
        return;
    }

    int freq[256] = {0};

    for (auto& c : comp_chunks)
        for (char ch : c.data)
            freq[(unsigned char)ch]++;

    HuffmanNode* root = buildTree(freq);

    unordered_map<char,string> codes;
    generateCodes(root, "", codes);

    processedChunks = 0;
    thread progressThread(progressBar, comp_numChunks);

    vector<thread> pool;//thread pool-implcit threading

    for (int i = 0; i < NUM_THREADS; i++)
        pool.push_back(thread(worker,ref(codes),i));
    for (int i = 0; i < comp_numChunks; i++) {
        comp_emptySlots.acquire();//semaphore used to prevent race condition
        comp_taskQueue.push({-comp_chunks[i].data.size(), i});
        comp_filledSlots.release();
    }

    comp_doneProducing = true;
    for (int i = 0; i < NUM_THREADS; i++)
        comp_filledSlots.release();

    for (auto& t : pool) t.join();

    progressThread.join();

    vector<unsigned char> packed;
    unsigned char buf = 0;
    int bitCnt = 0, totalBits = 0;

    for (auto& c : comp_chunks) {
        int taken = 0;
        for (auto byte : c.bitString) {
            for (int b = 7; b >= 0; b--) {
                if (taken == c.bitCount) break;
                int bit = (byte >> b) & 1;

                buf <<= 1;
                buf |= bit;

                bitCnt++;
                totalBits++;
                taken++;

                if (bitCnt == 8) {
                    packed.push_back(buf);
                    buf = 0;
                    bitCnt = 0;
                }
            }
        }
    }

    if (bitCnt > 0) {
        buf <<= (8 - bitCnt);
        packed.push_back(buf);
    }

    vector<int> sizes(comp_numChunks);
    for (int i = 0; i < comp_numChunks; i++)
        sizes[i] = comp_chunks[i].bitCount;

    saveFile(out, freq, sizes, packed, totalBits);
    double ratio =
    100.0 *
    (1.0 -
    ((double)packed.size() /
    (double)comp_fileSize));

cout << "\nOriginal Size   : "
     << comp_fileSize
     << " bytes";

cout << "\nCompressed Size : "
     << packed.size()
     << " bytes";

cout << "\nCompression Ratio: "
     << fixed
     << setprecision(2)
     << ratio
     << "%\n";
    cout << "\nCompression done\n";
}

// ================= DECOMPRESSION =================
void decompressChunk(HuffmanNode* root,
                     int idx,
                     int offset,
                     int bits)
{
    string result = "";

    HuffmanNode* curr = root;

    for (int i = 0; i < bits; i++) {
        int globalBit = offset + i;
        int byteIndex = globalBit / 8;
        int bitPos = 7 - (globalBit % 8);

        int bit = (decomp_data[byteIndex] >> bitPos) & 1;

        if (bit == 0) curr = curr->left;
        else curr = curr->right;

        if (!curr->left && !curr->right) {
            result += curr->ch;
            curr = root;
        }
    }

    decomp_chunks[idx] = result;
}

void decompWorker(HuffmanNode* root,vector<int>& offsets,int threadID){
    while (true) {
        decomp_filledSlots.acquire();

        unique_lock<mutex> lock(decomp_mtx);

        if (decomp_taskQueue.empty() && decomp_doneProducing)
            break;

        if (decomp_taskQueue.empty())
            continue;

        int idx = decomp_taskQueue.front();
        threadStatus[threadID] ="Decompressing chunk " + to_string(idx);
        decomp_taskQueue.pop();

        lock.unlock();
        decomp_emptySlots.release();
activeThreads++;

decompressChunk(root, idx, offsets[idx], decomp_chunkSizes[idx]);

processedChunks++;

activeThreads--;
    }
}

bool readCompressed(const string& path) {
    ifstream in(path, ios::binary);
    if (!in) return false;

    in.read((char*)decomp_freq, 256*sizeof(int));
    in.read((char*)&decomp_numChunks, sizeof(int));

    decomp_chunkSizes.resize(decomp_numChunks);
    in.read((char*)decomp_chunkSizes.data(), decomp_numChunks*sizeof(int));

    in.read((char*)&decomp_totalBits, sizeof(int));
    if (decomp_numChunks <= 0 || decomp_totalBits <= 0) {
      cout << "Invalid compressed file\n";
      return false;
    }
    unsigned char byte;
    while (in.read((char*)&byte, 1))
        decomp_data.push_back(byte);

    return true;
}

// ================= RUN DECOMPRESSION =================
void runDecompression() {
    processedChunks = 0;
    compressedBytes = 0;
activeThreads = 0;
decomp_data.clear();
decomp_chunkSizes.clear();
decomp_chunks.clear();
decomp_taskQueue = queue<int>();
decomp_doneProducing = false;
processedChunks = 0;
    string in, out;

cout << "Enter compressed file (.bin): ";
getline(cin >> ws, in);

if (!fileExists(in)) {
    cout << "Compressed file does not exist\n";
    return;
}

if (!hasBinExtension(in)) {
    cout << "Input compressed file must be .bin\n";
    return;
}

cout << "Enter output file [Press Enter for default]: ";
getline(cin, out);

if (out.empty()) {
    out = generateDecompressedPath(in);
    cout << "Auto generated output: " << out << endl;
    this_thread::sleep_for(chrono::seconds(10));
}
    decomp_data.clear();
    decomp_chunkSizes.clear();

    if (!readCompressed(in)) {
        cout << "Error reading file\n";
        return;
    }

    HuffmanNode* root = buildTree(decomp_freq);

    decomp_chunks.resize(decomp_numChunks);

    vector<int> offsets(decomp_numChunks);
    offsets[0] = 0;
    for (int i = 1; i < decomp_numChunks; i++)
        offsets[i] = offsets[i-1] + decomp_chunkSizes[i-1];

    processedChunks = 0;
    thread progressThread(progressBar, decomp_numChunks);

    vector<thread> pool;

    for (int i = 0; i < NUM_THREADS; i++)
    pool.push_back(thread(decompWorker,root,ref(offsets),i));

    for (int i = 0; i < decomp_numChunks; i++) {
        decomp_emptySlots.acquire();
        decomp_taskQueue.push(i);
        decomp_filledSlots.release();
    }

    decomp_doneProducing = true;

    for (int i = 0; i < NUM_THREADS; i++)
        decomp_filledSlots.release();

    for (auto& t : pool) t.join();
    // reset for next run
decomp_doneProducing = false;
    progressThread.join();

    string finalOutput = "";
    for (auto& s : decomp_chunks)
        finalOutput += s;

    ofstream file(out);
    file << finalOutput;
    file.close();

    cout << "\nDecompression done\n";
}
// ================= MAIN =================
int main() {
    while (true) {
        system("clear");

        cout << CYAN;
        cout << "╔══════════════════════════════════════════════════════════╗\n";
        cout << "║         HUFFMAN MULTITHREADED FILE COMPRESSOR            ║\n";
        cout << "╚══════════════════════════════════════════════════════════╝\n";
        cout << RESET;

        cout << "\n";
        cout << "  " << YELLOW << "┌─────────────────────────────┐" << RESET << "\n";
        cout << "  " << YELLOW << "│        SELECT OPERATION      │" << RESET << "\n";
        cout << "  " << YELLOW << "├─────────────────────────────┤" << RESET << "\n";
        cout << "  " << YELLOW << "│" << RESET << "   " << GREEN  << "[1]" << RESET << "  Compress a File       " << YELLOW << "│" << RESET << "\n";
        cout << "  " << YELLOW << "│" << RESET << "   " << GREEN  << "[2]" << RESET << "  Decompress a File     " << YELLOW << "│" << RESET << "\n";
        cout << "  " << YELLOW << "│" << RESET << "   " << RED    << "[3]" << RESET << "  Exit                  " << YELLOW << "│" << RESET << "\n";
        cout << "  " << YELLOW << "└─────────────────────────────┘" << RESET << "\n";

        cout << "\n  " << CYAN << ">> " << RESET << "Your choice: ";

        int c;
        cin >> c;
        cin.ignore();

        if      (c == 1) runCompression();
        else if (c == 2) runDecompression();
        else if (c == 3) {
            system("clear");
            cout << CYAN;
            cout << "╔══════════════════════════════════════════════════════════╗\n";
            cout << "║                      GOODBYE!                            ║\n";
            cout << "╚══════════════════════════════════════════════════════════╝\n";
            cout << RESET << "\n";
            break;
        } else {
            cout << RED << "\n  [!] Invalid choice. Press Enter to try again..." << RESET;
            cin.get();
        }
    }
    return 0;
}
