//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include <algorithm>
#include <string>
#include "TypeDescription.h"
#include<iostream>

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports LONG decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding) : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding)
{
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding) : ColumnVector(len, encoding)
{
    // decimal column vector has no encoding so we don't allocate memory to this->vector
    this->vector = nullptr;
    posix_memalign(reinterpret_cast<void **>(&vector), 32,
                   len * sizeof(int64_t));
    memoryUsage += (uint64_t)sizeof(uint64_t) * len;

    std::string str_precision = std::to_string(precision);
    if (precision < 1)
    {
        throw InvalidArgumentException("precision " + str_precision + " is negative.");
    }
    this->precision = precision;

    std::string str_scale = std::to_string(scale);
    if (scale < 0)
    {
        throw InvalidArgumentException("scale " + str_scale + " is negative.");
    }
    else if (scale > precision)
    {
        throw InvalidArgumentException("precision " + str_precision + " is smaller than scale " + str_scale);
    }
    this->scale = scale;
}

void DecimalColumnVector::close()
{
    if (!closed)
    {
        ColumnVector::close();
        if (encoding && vector)
        {
            // free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount)
{
    for (int i = 0; i < rowCount; i++)
    {
        std::cout << vector[i] << std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector()
{
    DecimalColumnVector::close();
}

void *DecimalColumnVector::current()
{
    if (vector == nullptr)
    {
        return nullptr;
    }
    else
    {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision()
{
    return precision;
}

int DecimalColumnVector::getScale()
{
    return scale;
}

// 获取scale(小数点后面的数字个数)
int StrDecimal::scale()
{
    size_t pos = decimal.find('.');
    if (pos != std::string::npos)
    {
        return decimal.length() - pos - 1;
    }
    return 0;
}

// 获取precision(总长度, 不包括小数点和前导0)
int StrDecimal::precision()
{
    int tot_len = decimal.length();
    size_t pos = decimal.find('.');
    if (pos != std::string::npos)
    {
        tot_len--;
    }
    // 去掉前导0
    int to_delete = 0;
    for (int i = 0; i < tot_len; i++)
    {
        if (decimal[i] == '0')
        {
            to_delete++;
        }
        else
            break;
    }
    tot_len -= to_delete;
    return tot_len;
}

// 对字符串进行四舍五入
std::string StrDecimal::roundDecimal(int target_scale)
{
    size_t dotPos = decimal.find('.');
    if (dotPos == std::string::npos)
    {
        // 没有小数点，直接补 0 返回
        if (target_scale)
        {
            decimal += ".";
            decimal.append(target_scale, '0');
        }
        return decimal;
    }

    std::string intPart = decimal.substr(0, dotPos);
    std::string fracPart = decimal.substr(dotPos + 1);

    // 如果小数位数小于 target_scale，直接补 0 返回
    if (fracPart.length() <= static_cast<size_t>(target_scale))
    {
        fracPart.append(target_scale - fracPart.length(), '0');
        return intPart + "." + fracPart;
    }

    // 判断是否需要四舍五入
    bool roundUp = fracPart[target_scale] >= '5';
    // 截断到指定小数位
    fracPart = fracPart.substr(0, target_scale);// scale=0?
    if (roundUp)
    {
        // 从右向左处理小数部分进位
        for (int i = target_scale - 1; i >= 0; --i)
        {
            if (fracPart[i] == '9')
            {
                fracPart[i] = '0';
            }
            else
            {
                fracPart[i] += 1;
                roundUp = false;
                break;
            }
        }

        // 如果小数部分全进位，需要处理整数部分进位
        if (roundUp)
        {
            int carry = 1;
            for (int i = intPart.size() - 1; i >= 0 && carry > 0; --i)
            {
                if (intPart[i] == '9')
                {
                    intPart[i] = '0';//99.99?
                }
                else
                {
                    intPart[i] += 1;
                    carry = 0;
                }
            }
            if (carry)
            {
                intPart = "1" + intPart;
            }
        }
    }
    return fracPart.empty() ? intPart : intPart + "." + fracPart;
}

long StrDecimal::longValue()
{
    size_t dotPos = decimal.find('.');
    if (dotPos == std::string::npos)
    {
        return stol(decimal);
    }   
    decimal.erase(std::remove(decimal.begin(), decimal.end(), '.'), decimal.end());
    return stol(decimal);
}

void DecimalColumnVector::add(std::string &value)
{
    if (writeIndex >= length)
    {
        ensureSize(writeIndex * 2, true);
    }
    // 转为指定scale
    StrDecimal decimal(value);
    if (decimal.scale() != scale)
    {
        decimal.roundDecimal(scale);
    }
    if (decimal.precision() > precision)
    {
        throw InvalidArgumentException("value exceeds the allowed precision");
    }

    int index=writeIndex++;
    vector[index]=decimal.longValue();
    isNull[index]=false;
}

void DecimalColumnVector::add(float value)
{
    add((double)value);
}

void DecimalColumnVector::add(double value)
{
    if (writeIndex >= length)
    {
        ensureSize(writeIndex * 2, true);
    }

    std::string value_str=std::to_string(value);
    StrDecimal decimal(value_str);
    if (decimal.scale() != scale)
    {
        decimal.roundDecimal(scale);
    }
    if (decimal.precision() > precision)
    {
        throw InvalidArgumentException("value exceeds the allowed precision");
    }
    int index=writeIndex++;
    vector[index]=decimal.longValue();
    isNull[index]=false;
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    ColumnVector::ensureSize(size, preserveData);
    if (length < size)
    {
        long *oldVector = vector;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       size * sizeof(int64_t));
        if (preserveData)
        {
            std::copy(oldVector, oldVector + length, vector);
        }
        delete[] oldVector;
        memoryUsage += (long)sizeof(long) * (size - length);
        resize(size);
    }
}

