package site.ycsb.agent;
public class AgentResp{

  public int tid;
  public String llmResp;
  public String action;
  public String query;
  public long timeCost;
  public long tokenCost;

  public AgentResp(int tid_, String resp, String action_, String query_, long time_cost, long token_cost) {
    tid = tid_;
    llmResp = resp;
    action = action_;
    query = query_;
    timeCost = time_cost;
    tokenCost = token_cost;
  }
}