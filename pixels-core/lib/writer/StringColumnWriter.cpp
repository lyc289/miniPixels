/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

#include "writer/StringColumnWriter.h"

StringColumnWriter::StringColumnWriter(std::shared_ptr<TypeDescription> type,std::shared_ptr<PixelsWriterOption> writerOption):
ColumnWriter(type,writerOption),curPixelVector(pixelStride) {
 encodingUtils= std::make_shared<EncodingUtils>();
 runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
 if (runlengthEncoding)
 {
  encoder = std::make_unique<RunLenIntEncoder>();
 }
 startsArray=std::make_shared<DynamicIntArray>();
}


void StringColumnWriter::flush(){
 ColumnWriter::flush();
 flushStarts();
}

void StringColumnWriter::flushStarts() {
 int startsFieldOffset=outputStream->getWritePos();
 startsArray->add(startOffset);
 if(byteOrder==ByteOrder::PIXELS_LITTLE_ENDIAN) {
  for (int i=0;i<startsArray->size();i++) {
   encodingUtils->writeIntLE(outputStream,startsArray->get(i));
  }
 }else {
  for(int i=0;i<startsArray->size();i++) {
   encodingUtils->writeIntBE(outputStream,startsArray->get(i));
  }
 }
 startsArray->clear();
 std::shared_ptr<ByteBuffer> offsetBuffer=std::make_shared<ByteBuffer>(4);
 offsetBuffer->putInt(startsFieldOffset);
 outputStream->putBytes(offsetBuffer->getPointer(),offsetBuffer->getWritePos());
}

int StringColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
// 	std::cout<<"In StringColumnWriter"<<std::endl;
//     auto columnVector = std::static_pointer_cast<BinaryColumnVector>(vector);
//     if (!columnVector)
//     {
//         throw std::invalid_argument("Invalid vector type");
//     }
//     duckdb::string_t* values=columnVector->vector;
//     int curPartLength;         // size of the partition which belongs to current pixel
//     int curPartOffset = 0;     // starting offset of the partition which belongs to current pixel
//     int nextPartLength = size; // size of the partition which belongs to next pixel
//     int* vLens = columnVector->lens;
//     int* vOffsets = columnVector->start;

//     // do the calculation to partition the vector into current pixel and next one
//     // doing this pre-calculation to eliminate branch prediction inside the for loop
//     while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)// 0 1 2
//     {
//         curPartLength = pixelStride - curPixelIsNullIndex;
//         writeCurPartWithoutDict(columnVector, values, vLens, vOffsets,curPartLength, curPartOffset);
//         newPixel();
//         curPartOffset += curPartLength;
//         nextPartLength = size - curPartOffset;
//     }

//     curPartLength = nextPartLength;
//     writeCurPartWithoutDict(columnVector, values, vLens, vOffsets,curPartLength, curPartOffset);
        
//     return outputStream->getWritePos();
}

void StringColumnWriter::writeCurPartWithoutDict(std::shared_ptr<ColumnVector> vector,uint8_t ** values, int* vLens,int* vOffsets,int curPartLength,int curPartOffset)
{

}

void StringColumnWriter::close()
{

}

void StringColumnWriter::newPixel()
{

}

bool StringColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption)
{

}

pixels::proto::ColumnEncoding StringColumnWriter::getColumnChunkEncoding()
{

}

