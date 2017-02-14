/*Linux sleep stored proc in 'C': */
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <unistd.h>
#include <sqludf.h>
#include <sqlsystm.h>
 
#define FALSE 0
#define TRUE 1
 
/* Main program */
 
void SQL_API_FN sleeper(SQLUDF_INTEGER *inint,
    SQLUDF_NULLIND *inintNullInd,
    SQLUDF_INTEGER *outDummy,
    SQLUDF_SMALLINT *dummyNullInd,
    SQLUDF_TRAIL_ARGS)
{
    if (*inintNullInd == -1) {
        *dummyNullInd = -1;
    }
    int j,k;
    k=*inint;
    j=sleep(k);
    *outDummy = 0;
    *dummyNullInd = 0;
}
