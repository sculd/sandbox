

class Solution:
    def solve(self):
        dp = [[0 for _ in range(232+1)] for _ in range(30+1)]
        for l in range(1, 31):
            for s in range(0, 232+1):
                dp[l][s] = dp[l-1][s] + (dp[l-1][s-l] if s-l >= 0 else 2 ** (l-1))

        return dp[30][232]

    def solve2(self):
        dp = [[0 for _ in range(232+1)] for _ in range(30+1)]
        dp[0] = [1 for _ in range(232+1)]

        for l in range(1, 31):
            for s in range(0, 232+1):
                dp[l][s] = dp[l-1][s] + (dp[l-1][s-l] if s-l >= 0 else 0)

        return dp[30][232]


s = Solution()
print(s.solve())
print(s.solve2())
print(s.solve() + s.solve2(), 2**30)
