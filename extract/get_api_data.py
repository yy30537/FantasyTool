from oauth_app import load_token, refresh_token_if_needed
import requests
import time

'''
通用函数：获取Yahoo Fantasy API数据
该函数会自动处理令牌的加载和刷新，并实现重试机制以应对网络请求失败或授权问题。

参数:
- url: API的请求URL
- max_retries: 最大重试次数，默认为3次

返回:
- 成功时返回API响应的JSON数据
- 失败时返回None，并打印错误信息

'''

def get_api_data(url, max_retries=3):
    """通用函数：获取Yahoo Fantasy API数据"""
    # 加载令牌
    token = load_token()
    if not token:
        print("未找到有效令牌")
        return None

    # 刷新令牌（如果需要）
    token = refresh_token_if_needed(token)
    if not token:
        print("令牌刷新失败")
        return None

    # 设置请求头
    headers = {
        'Authorization': f"Bearer {token['access_token']}",
        'Content-Type': 'application/json'
    }

    # 重试机制
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                # 授权问题，尝试刷新令牌
                print("授权失败，尝试刷新令牌...")
                token = refresh_token_if_needed(token)
                if token:
                    headers['Authorization'] = f"Bearer {token['access_token']}"
                    continue
                else:
                    print("令牌刷新失败，无法继续请求")
                    return None
            else:
                print(f"请求失败: {response.status_code} - {response.text}")
                # 如果不是最后一次尝试，等待后重试
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 指数退避
                    print(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                return None
        except Exception as e:
            print(f"请求时出错: {str(e)}")
            # 如果不是最后一次尝试，等待后重试
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 指数退避
                print(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
                continue
            return None

    return None