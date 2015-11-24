/**
 * Created by kevin on 2015/10/18.
 */
public class Params {
    public long rowNum = 1L;
    public long totalMg=0L;
    public long totalTime=0L;

    public long getTotalTime() {

        return System.currentTimeMillis() - totalTime;
    }

    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }

    public long getTotalMg() {
        return totalMg;
    }

    public void setTotalMg(long totalMg) {
        this.totalMg = totalMg;
    }

    public   long getRowNum() {
        return rowNum;
    }

    public   void setRowNum(long rowNum) {
        this.rowNum = rowNum;
    }


}
