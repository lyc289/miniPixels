//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <iostream>
#include <ctime>
#include <sstream>

DateColumnVector::DateColumnVector(uint64_t len, bool encoding) : ColumnVector(len, encoding)
{
	posix_memalign(reinterpret_cast<void **>(&dates), 32, len * sizeof(int32_t));
	memoryUsage += (long)sizeof(int) * len;
}

void DateColumnVector::close()
{
	if (!closed)
	{
		if (encoding && dates != nullptr)
		{
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount)
{
	for (int i = 0; i < rowCount; i++)
	{
		std::cout << dates[i] << std::endl;
	}
}

DateColumnVector::~DateColumnVector()
{
	if (!closed)
	{
		DateColumnVector::close();
	}
}

/**
 * Set a row from a value, which is the days from 1970-1-1 UTC.
 * We assume the entry has already been isRepeated adjusted.
 *
 * @param elementNum
 * @param days
 */
void DateColumnVector::set(int elementNum, int days)
{
	if (elementNum >= writeIndex)
	{
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
}

void *DateColumnVector::current()
{
	if (dates == nullptr)
	{
		return nullptr;
	}
	else
	{
		return dates + readIndex;
	}
}

// 对输入的时间修改day后 检查是否进位
void DateColumnVector::normalize_date(int& year, int& month, int& day)
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

int DateColumnVector::str2int(std::string &value)
{
	// 时区原因, 输入数据需要对齐
	int year, month, day;
	if (sscanf(value.c_str(), "%d-%d-%d", &year, &month, &day) != 3)
	{
		throw std::invalid_argument("Invalid date format, should be year-month-day");
	}
	std::tm time_input = {};
	day+=1;
	normalize_date(year, month, day);
	time_input.tm_year = year - 1900;
	time_input.tm_mon = month - 1;
	time_input.tm_mday = day;
	std::time_t cur_timestamp = mktime(&time_input);
	if (cur_timestamp == -1)
	{
		throw std::runtime_error("Failed to convert date to timestamp");
	}
	return static_cast<int>(cur_timestamp / (60 * 60 * 24));
}

/*
input date format is "YYYY-MM-DD" by default
*/
void DateColumnVector::add(std::string &value)
{
	int days = this->str2int(value);
	if (writeIndex > length)
	{
		ensureSize(writeIndex * 2, true);
	}
	this->set(writeIndex, days);
	isNull[writeIndex - 1] = false;
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData)
{
	ColumnVector::ensureSize(size, preserveData);
	if (length < size)
	{
		int *oldvector = this->dates;
		posix_memalign(reinterpret_cast<void **>(&dates), 32, size * sizeof(int32_t));
		if (preserveData)
		{
			std::copy(oldvector, oldvector + length, dates);
		}
		delete[] oldvector;
		memoryUsage += (long)sizeof(int32_t) * (size - length);
	}
}


