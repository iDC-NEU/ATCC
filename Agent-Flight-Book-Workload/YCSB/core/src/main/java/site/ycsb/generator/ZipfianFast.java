
package site.ycsb.generator;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 单线程、轻量版 Zipfian 实现，行为模仿DBx1000 代码（返回 1..n，需先调用 calculateDenom）
 */
public final class ZipfianFast {
  private final double theta;
  private double zeta2theta;
  private double denom = 0.0; // 对应 C++ denom (zetan)
  private long theN = 0;       // 对应 C++ the_n

  public ZipfianFast(double theta) {
    this.theta = theta;
    // 与 C++ init() 中的 zeta(2, theta)
    this.zeta2theta = zetaStatic(2, theta);
  }

  // 完整 zeta 求和（与 C++ 中 zeta 函数一样）
  public static double zetaStatic(long n, double theta) {
    double sum = 0.0;
    for (long i = 1; i <= n; i++) {
      sum += Math.pow(1.0 / i, theta);
    }
    return sum;
  }

  /**
   * 与 C++ calculateDenom() 等价：计算并设置 denom（zetan）。
   * 必须仅在初始化阶段调用一次（C++ 里 assert the_n==0）。
   */
  public void calculateDenom(long n) {
    if (n <= 0) {
      throw new IllegalArgumentException("n must be > 0");
    }
    if (this.theN != 0) {
      throw new IllegalStateException("denom already calculated");
    }
    this.theN = n;
    this.denom = zetaStatic(n, theta);
  }

  /**
   * 返回 1..n 的 zipf 值（模仿 C++ 实现）
   * 必须在 calculateDenom(...) 之后调用
   */
  public long zipf() {
    if (theN == 0 || denom <= 0.0) {
      throw new IllegalStateException("denom not initialized; call calculateDenom(n) first");
    }

    double alpha = 1.0 / (1.0 - theta);
    double zetan = denom;
    double eta = (1.0 - Math.pow(2.0 / theN, 1.0 - theta)) / (1.0 - zeta2theta / zetan);

    double u = ThreadLocalRandom.current().nextDouble();
    double uz = u * zetan;
    if (uz < 1.0) return 1;
    if (uz < 1.0 + Math.pow(0.5, theta)) return 2;

    return 1L + (long) (theN * Math.pow(eta * u - eta + 1.0, alpha));
  }

  // 简单示例
  public static void main(String[] args) {
    double theta = 0.99; // or your g_zipf_theta
    long n = 1000000L;   // example keyspace size
    ZipfianFast z = new ZipfianFast(theta);
    z.calculateDenom(n);
    // 生成若干样本
    for (int i = 0; i < 10; i++) {
      System.out.println(z.zipf());
    }
  }
}