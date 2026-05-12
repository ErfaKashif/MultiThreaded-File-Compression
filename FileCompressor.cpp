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
#include <atomic>
#include <sstream>
#include <cstdlib>
using namespace std;

// -------------------------------------------------------
//  SETTINGS
//  CHUNK_SIZE 1024 : 1 KB per chunk 
//  NUM_THREADS 2   : optimal for VirtualBox (1-2 cores)
//                    avoids context-switch overhead of 4
//                    threads fighting over 2 cores
// -------------------------------------------------------
#define CHUNK_SIZE  1024
#define NUM_THREADS 2
#define BUFFER_SIZE 4

#define GREEN  "\033[32m"
#define RED    "\033[31m"
#define YELLOW "\033[33m"
#define CYAN   "\033[36m"
#define RESET  "\033[0m"

// ================================================================
//  STRUCTS
// ================================================================
struct Chunk {
    int chunkID;
    string data;
    int bitCount;
    vector<unsigned char> bitString;
};

struct HuffmanNode {
    char ch; int freq;
    HuffmanNode *left, *right;
    HuffmanNode(char c, int f) : ch(c), freq(f), left(NULL), right(NULL) {}
};

struct Compare {
    bool operator()(HuffmanNode* a, HuffmanNode* b) {
        return a->freq > b->freq;
    }
};

// ================================================================
//  COMPRESSION GLOBALS
// ================================================================
vector<Chunk> comp_chunks;
long          comp_fileSize  = 0;
int           comp_numChunks = 0;

// Priority queue gives largest chunks highest priority
priority_queue<pair<int,int>> comp_taskQueue;
mutex comp_mtx;        // protects comp_taskQueue and comp_chunks writes
mutex comp_writeMutex; // mutex-protected output writing

// counting_semaphore implements bounded-buffer producer-consumer
counting_semaphore<BUFFER_SIZE> comp_emptySlots(BUFFER_SIZE);
counting_semaphore<BUFFER_SIZE> comp_filledSlots(0);
bool comp_doneProducing = false;

// ================================================================
//  DECOMPRESSION GLOBALS
// ================================================================
vector<string> decomp_chunks;
queue<int>     decomp_taskQueue;
mutex          decomp_mtx;

counting_semaphore<BUFFER_SIZE> decomp_emptySlots(BUFFER_SIZE);
counting_semaphore<BUFFER_SIZE> decomp_filledSlots(0);
bool decomp_doneProducing = false;

int                   decomp_freq[256];
vector<unsigned char> decomp_data;
vector<int>           decomp_chunkSizes;
int                   decomp_numChunks    = 0;
int                   decomp_totalBits    = 0;
long                  decomp_originalSize = 0;

// ================================================================
//  SHARED PROGRESS GLOBALS
// ================================================================
atomic<int>  processedChunks(0);
atomic<long> currentCompressedSize(0);
atomic<int>  activeThreads(0);
string       threadStatus[NUM_THREADS];
mutex        screenMutex;

// ================================================================
//  HELPERS
// ================================================================
void clearScreen() { system("clear"); }

void waitEnter() {
    cout << "\n  " << CYAN << "Press Enter to return to menu..." << RESET << flush;
    cin.get();
}

// ================================================================
//  FILE CHECKS
// ================================================================
bool fileExists(const string& path) {
    ifstream f(path, ios::binary);
    return f.good();
}

bool hasBinExtension(const string& path) {
    return path.size() >= 4 &&
           path.substr(path.size() - 4) == ".bin";
}

string generateCompressedPath(const string& p) {
    size_t s   = p.find_last_of("/\\");
    string dir = (s != string::npos) ? p.substr(0, s+1) : "";
    string f   = (s != string::npos) ? p.substr(s+1)    : p;
    return dir + f + ".bin";
}

string generateDecompressedPath(const string& p) {
    size_t s   = p.find_last_of("/\\");
    string dir = (s != string::npos) ? p.substr(0, s+1) : "";
    string f   = (s != string::npos) ? p.substr(s+1)    : p;
    if (f.size() >= 4 && f.substr(f.size()-4) == ".bin")
        f = f.substr(0, f.size()-4);
    return dir + "decompressed_" + f;
}

// ================================================================
//  HUFFMAN CORE
// ================================================================
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
        m->left = l; m->right = r;
        pq.push(m);
    }
    return pq.top();
}

void generateCodes(HuffmanNode* root, string code,
                   unordered_map<char,string>& codes) {
    if (!root) return;
    if (!root->left && !root->right) {
        codes[root->ch] = (code.empty() ? "0" : code);
        return;
    }
    generateCodes(root->left,  code + "0", codes);
    generateCodes(root->right, code + "1", codes);
}

void freeTree(HuffmanNode* root) {
    if (!root) return;
    freeTree(root->left);
    freeTree(root->right);
    delete root;
}
// ================================================================
void progressBar(int total, const string& op) {
    auto start = chrono::high_resolution_clock::now();

    while (processedChunks.load() < total) {
        {
            lock_guard<mutex> lk(screenMutex);
            clearScreen();

            int    done      = processedChunks.load();
            int    percent   = (total > 0) ? (done * 100) / total : 0;
            auto   now       = chrono::high_resolution_clock::now();
            double elapsed   = chrono::duration<double>(now - start).count();
            double speed     = (elapsed > 0) ? done / elapsed : 0;
            double remaining = (speed  > 0)  ? (total - done) / speed : 0;
            double ratio     = 0;
            if (comp_fileSize > 0)
                ratio = 100.0 * (1.0 -
                    ((double)currentCompressedSize.load() / (double)comp_fileSize));

            cout << CYAN
                 << "====================================================\n"
                 << "     MULTITHREADED FILE COMPRESSOR  [" << op << "]\n"
                 << "====================================================\n"
                 << RESET;

            cout << GREEN  << "Processed Chunks : " << done << "/" << total << RESET << "\n";
            cout << YELLOW << "Compressed Size  : " << fixed << setprecision(2)
                           << currentCompressedSize.load() / 1024.0 << " KB" << RESET << "\n";
            if (op == "COMPRESS")
                cout << GREEN << "Compression Ratio: " << ratio << "%" << RESET << "\n";
            cout << CYAN   << "Speed            : " << speed    << " chunks/sec" << RESET << "\n";
            cout << YELLOW << "ETA              : " << remaining << " sec"       << RESET << "\n";

            // Progress bar
            cout << "\nProgress:\n[";
            int bars = percent / 2;
            for (int i = 0; i < 50; i++)
                cout << (i < bars ? "#" : " ");
            cout << "] " << percent << "%\n";

            // Thread activity display
            cout << "\nThread Activity:\n";
            for (int i = 0; i < NUM_THREADS; i++)
                cout << "  Thread " << i+1 << " : "
                     << (threadStatus[i].empty() ? "idle" : threadStatus[i]) << "\n";
        }
        this_thread::sleep_for(chrono::milliseconds(150));
    }

    clearScreen();
    cout << GREEN
         << "====================================================\n"
         << "              OPERATION COMPLETE\n"
         << "====================================================\n"
         << RESET;
    sleep(2);
}

// ================================================================
//  THREADED FILE READING 
//  Each thread opens the file independently and seeks to its own
//  region.
//  lock_guard protects only the write into comp_chunks so disk
//  reads themselves remain parallel.
// ================================================================
void readChunkWorker(const string& filename, int index) {
    ifstream file(filename, ios::binary);
    long start = (long)index * CHUNK_SIZE;
    file.seekg(start);
    int size = (int)min((long)CHUNK_SIZE, comp_fileSize - start);
    string data(size, '\0');
    file.read(&data[0], size);

    // Mutex-protected write into shared vector
    lock_guard<mutex> lock(comp_mtx);
    comp_chunks[index].chunkID = index;
    comp_chunks[index].data    = data;
}

bool loadFile(const string& filename) {
    ifstream file(filename, ios::binary);
    if (!file) return false;
    file.seekg(0, ios::end);
    comp_fileSize = (long)file.tellg();
    if (comp_fileSize <= 0) { cout << "File is empty\n"; return false; }
    file.close();

    comp_numChunks = (int)((comp_fileSize + CHUNK_SIZE - 1) / CHUNK_SIZE);
    comp_chunks.resize(comp_numChunks);

    // One thread per chunk — parallel file reading
    vector<thread> readers;
    readers.reserve(comp_numChunks);
    for (int i = 0; i < comp_numChunks; i++)
        readers.emplace_back(readChunkWorker, filename, i);
    for (auto& t : readers) t.join();

    return true;
}

// ================================================================
//  COMPRESS ONE CHUNK
//  CRITICAL: the final partial-byte flush (if bits > 0) is
//  mandatory — without it the last byte of each chunk is lost
//  and decompression produces corrupted/truncated output.
// ================================================================
vector<unsigned char> compressChunk(const string& data,
                                     unordered_map<char,string>& codes,
                                     int& bitCount) {
    vector<unsigned char> result;
    unsigned char buffer = 0;
    int bits = 0;
    bitCount = 0;

    for (unsigned char c : data) {
        const string& code = codes[(char)c];
        bitCount += (int)code.size();
        for (char b : code) {
            buffer <<= 1;
            if (b == '1') buffer |= 1;
            if (++bits == 8) {
                result.push_back(buffer);
                buffer = 0; bits = 0;
            }
        }
    }
    // Flush remaining bits into final byte with zero padding on right
    if (bits > 0) {
        buffer <<= (8 - bits);
        result.push_back(buffer);
    }
    return result;
}

// ================================================================
//  COMPRESSION WORKER
//  Semaphore-controlled: acquire filledSlots before taking work,
//  release emptySlots after — implements bounded buffer
//  Priority queue: largest chunk processed first 
// ================================================================
void worker(unordered_map<char,string>& codes, int threadID) {
    while (true) {
        comp_filledSlots.acquire(); // block until task available

        unique_lock<mutex> lock(comp_mtx);
        if (comp_taskQueue.empty() && comp_doneProducing) { lock.unlock(); break; }
        if (comp_taskQueue.empty())                        { lock.unlock(); continue; }

        int index = comp_taskQueue.top().second; // largest chunk first
        comp_taskQueue.pop();
        threadStatus[threadID] = "Compressing chunk " + to_string(index);
        lock.unlock();

        comp_emptySlots.release(); // free a buffer slot for producer

        activeThreads++;
        comp_chunks[index].bitString = compressChunk(
            comp_chunks[index].data, codes, comp_chunks[index].bitCount);
        currentCompressedSize += (long)comp_chunks[index].bitString.size();
        processedChunks++;
        activeThreads--;
        threadStatus[threadID] = "idle";
    }
}

// ================================================================
//  SAVE COMPRESSED FILE
//  File layout (must match readCompressed exactly):
//    [256 x int]   frequency table       — rebuilds Huffman tree
//    [int]         numChunks
//    [N x int]     per-chunk bit counts  — drives offset calculation
//    [int]         totalBits
//    [long]        original file size    — used for validation
//    [bytes...]    packed compressed data
// ================================================================
void saveFile(const string& path, int* freq,
              vector<int>& sizes,
              vector<unsigned char>& data,
              int totalBits) {
    ofstream out(path, ios::binary);
    if (!out) { cout << RED << "Error writing file\n" << RESET; return; }

    out.write((char*)freq,            256 * sizeof(int));
    out.write((char*)&comp_numChunks, sizeof(int));
    out.write((char*)sizes.data(),    (int)sizes.size() * sizeof(int));
    out.write((char*)&totalBits,      sizeof(int));
    out.write((char*)&comp_fileSize,  sizeof(long));

    // Mutex-protected final data write (mandatory requirement)
    lock_guard<mutex> lock(comp_writeMutex);
    out.write((char*)data.data(), (int)data.size());
    out.close();
}

// ================================================================
//  RUN COMPRESSION
// ================================================================
void runCompression() {
    // Full reset — required so re-runs in same session work correctly
    comp_chunks.clear();
    comp_taskQueue      = priority_queue<pair<int,int>>();
    comp_doneProducing  = false;
    processedChunks     = 0;
    currentCompressedSize = 0;
    activeThreads       = 0;
    for (int i = 0; i < NUM_THREADS; i++) threadStatus[i] = "";
    
    new (&comp_emptySlots) counting_semaphore<BUFFER_SIZE>(BUFFER_SIZE);
    new (&comp_filledSlots) counting_semaphore<BUFFER_SIZE>(0);

    string in, out;
    cout << "\nEnter input file: ";
    getline(cin >> ws, in);

    if (!fileExists(in)) {
        cout << RED << "Input file does not exist\n" << RESET;
        sleep(2); waitEnter(); return;
    }

    cout << "Enter output file (.bin) [Enter for auto]: ";
    getline(cin, out);

    if (out.empty()) {
        out = generateCompressedPath(in);
        cout << YELLOW << "Auto output: " << out << RESET << "\n" << flush;
        sleep(3);
    }

    if (!hasBinExtension(out)) {
        cout << RED << "Output must have .bin extension\n" << RESET;
        sleep(2); waitEnter(); return;
    }

    if (!loadFile(in)) {
        cout << RED << "Error loading file\n" << RESET;
        sleep(2); waitEnter(); return;
    }

    // Build Huffman tree from entire file (global frequency table)
    int freq[256] = {0};
    for (auto& c : comp_chunks)
        for (unsigned char ch : c.data)
            freq[ch]++;

    HuffmanNode* root = buildTree(freq);
    if (!root) {
        cout << RED << "Huffman tree build failed\n" << RESET;
        sleep(2); waitEnter(); return;
    }

    unordered_map<char,string> codes;
    generateCodes(root, "", codes);

    processedChunks = 0;
    thread progressThread(progressBar, comp_numChunks, string("COMPRESS"));

    // Spawn thread pool
    vector<thread> pool;
    for (int i = 0; i < NUM_THREADS; i++)
        pool.emplace_back(worker, ref(codes), i);

    // Producer: enqueue all chunks — largest size first (priority scheduling)
    for (int i = 0; i < comp_numChunks; i++) {
        comp_emptySlots.acquire(); // wait for a free buffer slot
        {
            lock_guard<mutex> lock(comp_mtx);
            comp_taskQueue.push({-(int)comp_chunks[i].data.size(), i});
        }
        comp_filledSlots.release(); // signal workers
    }

    comp_doneProducing = true;
    // Wake all sleeping workers so they can see doneProducing and exit
    for (int i = 0; i < NUM_THREADS; i++)
        comp_filledSlots.release();

    for (auto& t : pool) t.join();
    progressThread.join();

    // Pack all chunks in chunk-ID order 0,1,2,...
    //workers process in priority order but output must be in index order so decompression offsets are correct
    vector<unsigned char> packed;
    unsigned char buf = 0;
    int bitCnt = 0, totalBits = 0;

    for (int i = 0; i < comp_numChunks; i++) {
        int taken = 0;
        for (unsigned char byte : comp_chunks[i].bitString) {
            for (int b = 7; b >= 0; b--) {
                if (taken == comp_chunks[i].bitCount) break;
                buf = (unsigned char)((buf << 1) | ((byte >> b) & 1));
                bitCnt++; totalBits++; taken++;
                if (bitCnt == 8) {
                    packed.push_back(buf);
                    buf = 0; bitCnt = 0;
                }
            }
            if (taken == comp_chunks[i].bitCount) break;
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
    freeTree(root);

    double ratio = 100.0 * (1.0 - ((double)packed.size() / (double)comp_fileSize));

    cout << GREEN  << "\nOriginal Size    : " << comp_fileSize << " bytes\n"  << RESET;
    cout << YELLOW << "Compressed Size  : " << packed.size()  << " bytes\n"  << RESET;
    cout << CYAN   << "Compression Ratio: " << fixed << setprecision(2)
                   << ratio << "%\n"                                          << RESET;
    cout << GREEN  << "Chunks Processed : " << comp_numChunks << "\n"        << RESET;
    cout << YELLOW << "Saved to         : " << out << "\n"                   << RESET;
    cout << "\nCompression done\n";
    waitEnter();
}

// ================================================================
//  DECOMPRESS ONE CHUNK
//  Uses per-chunk bit offset so each chunk is decoded independently.
//  offset[i] = sum of bitCounts of all chunks before i.
// ================================================================
void decompressChunk(HuffmanNode* root, int idx, int offset, int bits) {
    string result;
    result.reserve(CHUNK_SIZE);

    // Edge case: only one unique character in entire file
    if (!root->left && !root->right) {
        result.assign(bits, root->ch);
        decomp_chunks[idx] = result;
        return;
    }

    HuffmanNode* curr = root;
    for (int i = 0; i < bits; i++) {
        int globalBit = offset + i;
        int bit = (decomp_data[globalBit / 8] >> (7 - (globalBit % 8))) & 1;
        curr = (bit == 0) ? curr->left : curr->right;
        if (!curr->left && !curr->right) {
            result += curr->ch;
            curr = root;
        }
    }
    decomp_chunks[idx] = result;
}

// ================================================================
//  DECOMPRESSION WORKER
// ================================================================
void decompWorker(HuffmanNode* root, vector<int>& offsets, int threadID) {
    while (true) {
        decomp_filledSlots.acquire();

        unique_lock<mutex> lock(decomp_mtx);
        if (decomp_taskQueue.empty() && decomp_doneProducing) {
            lock.unlock(); break;
        }
        if (decomp_taskQueue.empty()) {
            lock.unlock(); continue;
        }

        int idx = decomp_taskQueue.front();
        decomp_taskQueue.pop();
        threadStatus[threadID] = "Decompressing chunk " + to_string(idx);
        lock.unlock();

        decomp_emptySlots.release();

        activeThreads++;
        decompressChunk(root, idx, offsets[idx], decomp_chunkSizes[idx]);
        processedChunks++;
        activeThreads--;
        threadStatus[threadID] = "idle";
    }
}

// ================================================================
//  READ COMPRESSED FILE
//  Must read fields in same order saveFile wrote them
// ================================================================
bool readCompressed(const string& path) {
    ifstream in(path, ios::binary);
    if (!in) return false;

    in.read((char*)decomp_freq,          256 * sizeof(int));
    in.read((char*)&decomp_numChunks,    sizeof(int));

    if (decomp_numChunks <= 0) {
        cout << RED << "Invalid compressed file (bad chunk count)\n" << RESET;
        return false;
    }

    decomp_chunkSizes.resize(decomp_numChunks);
    in.read((char*)decomp_chunkSizes.data(), decomp_numChunks * sizeof(int));
    in.read((char*)&decomp_totalBits,    sizeof(int));
    in.read((char*)&decomp_originalSize, sizeof(long));

    if (decomp_totalBits <= 0) {
        cout << RED << "Invalid compressed file (zero bits)\n" << RESET;
        return false;
    }

    decomp_data.clear();
    unsigned char byte;
    while (in.read((char*)&byte, 1))
        decomp_data.push_back(byte);

    in.close();
    return true;
}

// ================================================================
//  RUN DECOMPRESSION
// ================================================================
void runDecompression() {
    // Full reset
    decomp_data.clear();
    decomp_chunkSizes.clear();
    decomp_chunks.clear();
    decomp_taskQueue     = queue<int>();
    decomp_doneProducing = false;
    processedChunks      = 0;
    currentCompressedSize = 0;
    activeThreads        = 0;
    decomp_numChunks     = 0;
    decomp_totalBits     = 0;
    decomp_originalSize  = 0;
    for (int i = 0; i < NUM_THREADS; i++) threadStatus[i] = "";

    new (&decomp_emptySlots) counting_semaphore<BUFFER_SIZE>(BUFFER_SIZE);
    new (&decomp_filledSlots) counting_semaphore<BUFFER_SIZE>(0);

    string in, out;
    cout << "\nEnter compressed file (.bin): ";
    getline(cin >> ws, in);

    if (!fileExists(in)) {
        cout << RED << "File does not exist: " << in << "\n" << RESET;
        sleep(2); waitEnter(); return;
    }
    if (!hasBinExtension(in)) {
        cout << RED << "Input must be a .bin file\n" << RESET;
        sleep(2); waitEnter(); return;
    }

    cout << "Enter output file [Enter for auto]: ";
    getline(cin, out);

    if (out.empty()) {
        out = generateDecompressedPath(in);
        cout << YELLOW << "Auto output: " << out << RESET << "\n" << flush;
        sleep(3);
    }

    if (!readCompressed(in)) {
        cout << RED << "Error reading compressed file\n" << RESET;
        sleep(2); waitEnter(); return;
    }

    HuffmanNode* root = buildTree(decomp_freq);
    if (!root) {
        cout << RED << "Failed to rebuild Huffman tree\n" << RESET;
        sleep(2); waitEnter(); return;
    }

    decomp_chunks.resize(decomp_numChunks);

    // offset[i] = total bits before chunk i .This is what makes each chunk independently decodable
    vector<int> offsets(decomp_numChunks);
    offsets[0] = 0;
    for (int i = 1; i < decomp_numChunks; i++)
        offsets[i] = offsets[i-1] + decomp_chunkSizes[i-1];

    processedChunks = 0;
    thread progressThread(progressBar, decomp_numChunks, string("DECOMPRESS"));

    vector<thread> pool;
    for (int i = 0; i < NUM_THREADS; i++)
        pool.emplace_back(decompWorker, root, ref(offsets), i);

    // Producer: push indices in order — ordering is preserved in decomp_chunks
    for (int i = 0; i < decomp_numChunks; i++) {
        decomp_emptySlots.acquire();
        {
            lock_guard<mutex> lock(decomp_mtx);
            decomp_taskQueue.push(i);
        }
        decomp_filledSlots.release();
    }

    decomp_doneProducing = true;
    for (int i = 0; i < NUM_THREADS; i++)
        decomp_filledSlots.release();

    for (auto& t : pool) t.join();
    progressThread.join();

    // Write output in binary mode preserving every byte exactly
    ofstream file(out, ios::binary);
    if (!file) {
        cout << RED << "Cannot write output: " << out << "\n" << RESET;
        freeTree(root); sleep(2); waitEnter(); return;
    }
    long totalWritten = 0;
    for (int i = 0; i < decomp_numChunks; i++) {
        file.write(decomp_chunks[i].data(), (long)decomp_chunks[i].size());
        totalWritten += (long)decomp_chunks[i].size();
    }
    file.close();
    freeTree(root);

    cout << GREEN  << "\nDecompression complete!\n"                               << RESET;
    cout << YELLOW << "Output File    : " << out << "\n"                          << RESET;
    cout << GREEN  << "Output Size    : " << totalWritten << " bytes\n"           << RESET;
    if (decomp_originalSize > 0)
        cout << CYAN << "Expected Size  : " << decomp_originalSize << " bytes\n" << RESET;
    if (decomp_originalSize > 0 && totalWritten == decomp_originalSize)
        cout << GREEN << "Validation     : PASSED (sizes match)\n"                << RESET;
    else if (decomp_originalSize > 0)
        cout << RED   << "Validation     : WARNING size mismatch\n"               << RESET;
    cout << YELLOW << "Verify with    : diff <original> " << out << "\n"          << RESET;
    waitEnter();
}

// ================================================================
//  MAIN MENU
// ================================================================
int main() {
    while (true) {
        clearScreen();
        cout << CYAN
             << "╔══════════════════════════════════════════════════════════╗\n"
             << "║         HUFFMAN MULTITHREADED FILE COMPRESSOR            ║\n"
             << "╚══════════════════════════════════════════════════════════╝\n"
             << RESET << "\n";

        cout << "  " << YELLOW << "┌─────────────────────────────┐" << RESET << "\n"
             << "  " << YELLOW << "│        SELECT OPERATION      │" << RESET << "\n"
             << "  " << YELLOW << "├─────────────────────────────┤" << RESET << "\n"
             << "  " << YELLOW << "│" << RESET << "   " << GREEN << "[1]" << RESET
             << "  Compress a File       " << YELLOW << "│" << RESET << "\n"
             << "  " << YELLOW << "│" << RESET << "   " << GREEN << "[2]" << RESET
             << "  Decompress a File     " << YELLOW << "│" << RESET << "\n"
             << "  " << YELLOW << "│" << RESET << "   " << RED   << "[3]" << RESET
             << "  Exit                  " << YELLOW << "│" << RESET << "\n"
             << "  " << YELLOW << "└─────────────────────────────┘" << RESET << "\n"
             << "\n  " << CYAN << ">> " << RESET << "Your choice: ";

        int c;
        cin >> c;
        cin.ignore();

        if      (c == 1) runCompression();
        else if (c == 2) runDecompression();
        else if (c == 3) {
            clearScreen();
            cout << CYAN
                 << "╔══════════════════════════════════════════════════════════╗\n"
                 << "║                      GOODBYE!                            ║\n"
                 << "╚══════════════════════════════════════════════════════════╝\n"
                 << RESET << "\n";
            break;
        } else {
            cout << RED << "\n  [!] Invalid choice." << RESET;
            sleep(1);
        }
    }
    return 0;
}
