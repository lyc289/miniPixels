//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include <iostream>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <chrono>

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding) : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding)
{
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding) : ColumnVector(len, encoding)
{
    this->precision = precision;
    posix_memalign(reinterpret_cast<void **>(&this->times), 64, len * sizeof(long));
}

void TimestampColumnVector::close()
{
    if (!closed)
    {
        ColumnVector::close();
        if (encoding && this->times != nullptr)
        {
            free(this->times);
        }
        this->times = nullptr;
    }
}

TimestampColumnVector::~TimestampColumnVector()
{
    if (!closed)
    {
        TimestampColumnVector::close();
    }
}

void *TimestampColumnVector::current()
{
    if (this->times == nullptr)
    {
        return nullptr;
    }
    else
    {
        return this->times + readIndex;
    }
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts)
{
    if (elementNum >= writeIndex)
    {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
}

// 对输入的时间修改day后 检查是否进位
void TimestampColumnVector::normalize_date(int& year, int& month, int& day)
{
	int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))
	{
		days_in_month[1] = 29; 
	}

	if (day > days_in_month[month-1])
	{
		day = 1;
		month += 1;
		if (month > 12)
		{
			month = 1;
			year += 1;
		}
	}
}

long TimestampColumnVector::str2long(std::string &value)
{
    // 时区问题, 实际时间要加18小时
    // 2005-10-01 23:58:19
    int year, month, day, hour, minute, second;
    std::istringstream ss(value);
    if (sscanf(value.c_str(), "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second) != 6)
	{
		throw std::invalid_argument("Invalid timestamp format, should be year-month-day hour:minute:second");
	}
    hour += 18;
    if (hour >= 24)
    {
        hour%=24;
        day++;
        normalize_date(year, month, day);
    }
    std::tm curtime = {};
    curtime.tm_year = year - 1900;
    curtime.tm_mon = month - 1;
    curtime.tm_mday = day;
    curtime.tm_hour = hour;
    curtime.tm_min = minute;
    curtime.tm_sec = second;
    std::time_t curtimestamp = mktime(&curtime);
    if (curtimestamp == -1)
	{
		throw std::runtime_error("Failed to convert to timestamp");
	}
    return static_cast<long>(curtimestamp) * 1000000;
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    ColumnVector::ensureSize(size, preserveData);
    if (length < size)
    {
        long *oldvector = this->times;
        posix_memalign(reinterpret_cast<void **>(&times), 32, size * sizeof(int64_t));
        if (preserveData)
        {
            std::copy(oldvector, oldvector + length, times);
        }
        delete[] oldvector;
        memoryUsage += (long)sizeof(int64_t) * (size - length);
    }
}

void TimestampColumnVector::add(std::string &value)
{
    long curTime = this->str2long(value);
    if (writeIndex >= length)
    {
        ensureSize(writeIndex * 2, true);
    }
    this->set(writeIndex, curTime);
    isNull[writeIndex - 1] = false;
}
