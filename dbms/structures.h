#ifndef STRUCTURES_H
#define STRUCTURES_H

#include <string>
#include <type_traits>

using namespace std;

template <typename T>
struct NodeD{
    T data;
    NodeD* next = nullptr;
    NodeD* prev = nullptr;
};

template <typename T>
struct List{
    int length = 0;
    NodeD<T>* head = new NodeD<T>;
    NodeD<T>* tail = head;

    void push(T data){
        NodeD<T>* new_node = new NodeD<T>{data, nullptr, tail};
        tail -> next = new_node;
        tail = new_node;
        length += 1;
    }

    T operator[](int index){
        return get(index);
    }

    T pop(){
        if (length == 0) throw runtime_error("List is empty");

        NodeD<T>* last = tail;

        T data = last -> data;
        tail -> prev -> next = nullptr;
        tail = tail -> prev;

        length --;

        delete last;
        return data;
    }

    T get(int index){
        if (index >= length || index < 0) throw runtime_error("Out of range");

        NodeD<T>* tek = head -> next;
        while (index != 0){
            tek = tek -> next;
            index --;
        }
        return tek -> data;
    }

    int find(T data){
        
        for (int index = 0; index < length; index++){
            if (get(index) == data){
                return index;
            }
        }

        return -1;
    }

    void del(int index){
        if (index >= length || index < 0) throw runtime_error("Out of range");
        if (index == length - 1) pop();

        NodeD<T>* tek = head -> next;
        while (index != 0){
            tek = tek -> next;
            index --;
        }

        NodeD<T>* pred = tek -> prev;
        pred -> next = tek -> next;
        pred -> next -> prev = pred;

        delete tek;
        length --;

    }

    void remove(T data){
        int index = find(data);

        if(index == -1) throw runtime_error("No value");

        del(index);
    }

    void insert(T data, int index){
        if (index > length || index < 0) throw runtime_error("Out of range");

        if (index == length){
            push(data);
        }

        else if (index >= 0){
            NodeD<T>* new_node = new NodeD<T>;
            new_node -> data = data;
            NodeD<T>* tek = head -> next;

            while (index != 0){
                tek = tek -> next;
                index --;
            }

            new_node -> prev = tek -> prev;
            tek -> prev -> next = new_node;
            tek -> prev = new_node;
            new_node -> next = tek;
            length++;
        }

    }

    void clear(){
        NodeD<T>* prev;
        NodeD<T>* tek = tail;
        while (tek != head){
            prev = tek ->prev;
            delete tek;
            tek = prev;
        }
        delete tek;
    }
};


template <typename T>
struct Array{

    struct Node{
        T value;
        bool state = false;
    };
    int arSize;
    Node* head;

    Array(int size){
        arSize = size;
        head = new Node[size];
    }

    void set(T data, int index){
        if (index >= arSize) throw runtime_error("Out of range");
        head[index].value = data;
        head[index].state = true;
    }

    T operator[](int index){
        if (index >= arSize) throw runtime_error("Out of range");
        if (!head[index].state) throw runtime_error("No value");
        return head[index].value;
    }

    void del(int index){
        if (index >= arSize) throw runtime_error("Out of range");
        if (!head[index].state) throw runtime_error("No value");
        head[index].state = false;
    }

    int find(T data){
        for (int i = 0; i < arSize; i++){
            if(!head[i].state) continue;

            if(head[i].value == data){
                return i;
            }
        }

        return -1;
    }

    void remove(T data){
        int index = find(data);
        if  (index == -1) throw runtime_error("No value");

        head[index].state = false;
    }

    ~Array(){
        delete[] head;
    }

};

template <typename T>
struct NodeH{
    string key;
    T value;
    bool state = false;
    bool deleted = false;
};


template <typename T>
struct HashTable{

    int size = 8;

    NodeH<T>* arr = new NodeH<T>[size];

    ~HashTable(){
        delete [] arr;
    }

    int hashFunc(string key){
        int hash_result = 0;
        for (int i = 0; i < key.size(); i++){
            hash_result = ((size - 1) * hash_result + key[i]) % size;
            hash_result = (hash_result * 2 + 1) % size;
        }
        return hash_result;
    }

    void Add(string key, T value){
        int index = hashFunc(key);
        
        while (index < size){
            if (!arr[index].state){
                arr[index].key = key;
                arr[index].value = value;
                arr[index].state = true;
                return;
            }
            else if(arr[index].key == key){
                arr[index].value = value;
                return;
            }
            index++;
        }

        Resize();
        Add(key, value);
    }

    void Resize(){
        size *= 2;
        NodeH<T>* oldArr = arr;

        arr = new NodeH<T>[size];

        for(int i = 0; i < size / 2; i++){
            if (oldArr[i].state && !oldArr[i].deleted){
                Add(oldArr[i].key, oldArr[i].value);
            }
        }

        delete[] oldArr;
    }

    T Get(string key){
        int index = hashFunc(key);
        
        while (index < size){
            if (arr[index].state && arr[index].key == key){
                return arr[index].value;
            }
            else if (arr[index].key == key && arr[index].deleted) throw runtime_error("No such key");
            else if (!arr[index].deleted && !arr[index].state) throw runtime_error("No such key");
            index++;
        }

        throw runtime_error("No such key");
    }

    void Remove(string key){
        int index = hashFunc(key);
        while (index < size){
            if (arr[index].state && arr[index].key == key){
                if (is_same<T, List<string>>::value) arr[index].value.clear();
                arr[index].state = false;
                arr[index].deleted = true;
                return;
            }
            else if (arr[index].key == key && arr[index].deleted) throw runtime_error("No such key");
            else if (!arr[index].deleted && !arr[index].state) throw runtime_error("No such key");
            index++;
        }
        throw runtime_error("No such key");
    }
};

#endif