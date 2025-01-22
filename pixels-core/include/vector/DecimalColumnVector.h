//
// Created by yuly on 05.04.23.
//

#ifndef PIXELS_DECIMALCOLUMNVECTOR_H
#define PIXELS_DECIMALCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include <string>

class StrDecimal
{
private:
    std::string decimal;
public:
    StrDecimal(std::string &num):decimal(num) {}
    std::string roundDecimal(int target_scale);
    int precision();
    int scale();
    long longValue();
};

class DecimalColumnVector: public ColumnVector {
public:
    long * vector;
    int precision;
    int scale;
    static long DEFAULT_UNSCALED_VALUE;
    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    DecimalColumnVector(int precision, int scale, bool encoding = false);
    DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding = false);
    ~DecimalColumnVector();
    void print(int rowCount) override;
    void close() override;
    void * current() override;
	int getPrecision();
	int getScale();

    void add(std::string &value) override;
    void add(float value) override;
    void add(double value) override;
    void ensureSize(uint64_t size, bool preserveData) override;
};

#endif //PIXELS_DECIMALCOLUMNVECTOR_H