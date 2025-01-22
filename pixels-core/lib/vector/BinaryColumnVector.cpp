//
// Created by liyu on 3/17/23.
//

#include "vector/BinaryColumnVector.h"
#include<iostream>
BinaryColumnVector::BinaryColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
    posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(duckdb::string_t));
    posix_memalign(reinterpret_cast<void **>(&start), 32, len*sizeof(uint32_t));
    posix_memalign(reinterpret_cast<void **>(&lens), 32, len*sizeof(uint32_t));
    memoryUsage += (long) sizeof(uint8_t) * len;
    nextFree = 0;
}

void BinaryColumnVector::close() {
	if(!closed) {
		ColumnVector::close();
		free(vector);
        free(start);
        free(lens);
		vector = nullptr;
        start = nullptr;
        lens = nullptr;
	}
}

void BinaryColumnVector::initBuffer(int size)
{
    nextFree=0;
    smallBufferNextFree=0;
    // if buffer is already allocated, keep using it, don't re-allocate
    if (buffer)
    {
        // Free up any previously allocated buffers that are referenced by vector
        if (bufferAllocationCount > 0)
        {
            for (int i = 0; i < length; ++i) 
            {
                vector[i] = nullptr;
            }
            buffer=smallBuffer;
        }
    }
    else 
    {
        // allocate a little extra space to limit need to re-allocate
        int bufferSize = length * (int) (size * EXTRA_SPACE_FACTOR);
        if (bufferSize < DEFAULT_BUFFER_SIZE)
        {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }
        buffer = new uint8_t[bufferSize];
        memoryUsage += sizeof(uint8_t) * bufferSize;
        smallBuffer = buffer;
    }
    bufferAllocationCount = 0;
}

void BinaryColumnVector::setRef(int elementNum, uint8_t * const &sourceBuf, int start, int length) 
{
    if(elementNum >= writeIndex) 
    {
        writeIndex = elementNum + 1;
    }
    this->vector[elementNum] = duckdb::string_t((char *)(sourceBuf + start), length);
    isNull[elementNum]=(sourceBuf==nullptr);
}

void BinaryColumnVector::setVal(int elementNum, uint8_t* sourceBuf, int start, int length) 
{
    setRef(elementNum, sourceBuf, start, length);
}


void BinaryColumnVector::print(int rowCount) {
	throw InvalidArgumentException("not support print binarycolumnvector.");
}

BinaryColumnVector::~BinaryColumnVector() {
	if(!closed) {
		BinaryColumnVector::close();
	}
}
void * BinaryColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

void BinaryColumnVector::add(std::string &value) {
    size_t len = value.size();
    uint8_t* buffer = new uint8_t[len];
    std::memcpy(buffer, value.data(), len);
    add(buffer,len);
    delete[] buffer;
}

void BinaryColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    ColumnVector::ensureSize(size, preserveData);
    if (size > length)
    {
        int* oldStart=start;
        posix_memalign(reinterpret_cast<void **>(&start), 32, size*sizeof(uint32_t));
        int* oldLength = lens;
        posix_memalign(reinterpret_cast<void **>(&lens), 32, size*sizeof(uint32_t));
        duckdb::string_t* oldVector = vector;
        posix_memalign(reinterpret_cast<void **>(&vector), 32, size*sizeof(duckdb::string_t));
        if (preserveData)
        {
            std::copy(oldVector, oldVector+length, vector);
            std::copy(oldStart, oldStart+length, start);
            std::copy(oldLength, oldLength+length, lens);
        }
        delete[] oldStart;
        delete[] oldLength;
        delete[] oldVector;
        memoryUsage += (long)sizeof(uint32_t)*size*2;
        resize(size);
    }
}

void BinaryColumnVector::add(uint8_t *v,int len) {
    if(writeIndex>=length) 
    {
        ensureSize(writeIndex*2,true);
    }
    setVal(writeIndex++,v,0,len);
}

