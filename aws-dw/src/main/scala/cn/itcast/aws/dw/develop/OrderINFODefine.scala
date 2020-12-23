package cn.itcast.aws.dw.develop

case class OrderINFODefine(
                            // order metadata
                            unique_id: String, // 订单id + _ + 商品barcode，作为唯一ID标识，用以在Redshift中剔除重复数据的影响。
                            order_id: String, // 订单id
                            order_date: String, // 订单日期 2020-01-01格式
                            order_ts: Long, // timestamp

                            // 店铺相关内容
                            store_district: String, // 店铺区域，如昌平区
                            store_province: String, // 省
                            store_city: String, // city
                            store_gps_longitude: String, // gps 经度
                            store_gps_latitude: String, // gps 纬度
                            store_created_ts: Long, // 店铺创建timestamp
                            store_address: String, // 店铺地址
                            store_gps_address: String, // 店铺gps地址
                            store_own_user_id: Int, // 店铺店主id
                            store_own_user_name: String, // 店铺店主名称
                            store_name: String, // 店铺名称
                            store_own_user_tel: Long, // 店铺店主手机号
                            store_id: Int, // 店铺id
                            is_signed: Int, // 是否开通移动支付

                            // 订单金额相关
                            discount: Double, // 折扣金额
                            discount_rate: Double, // 折扣率
                            product_count: Double, // 本单商品卖出个数
                            pay_type: String, // 支付类型，cash alipay wechat
                            small_change: Double, // 找零
                            receivable: Double, // 应收
                            after_discount: Double, // 打折后金额
                            total_no_discount: Double, // 总金额（打折前）
                            erase: Double, // 抹零金额
                            pay_total: Double, // 总付款
                            member_id: String, // 会员ID

                            // 商品相关
                            product_name: String, // 商品名称
                            sold_count: Double, // 售出数量
                            barcode: String, // 商品条码
                            price: Double, // 售出单价
                            retail_price: Double, // 标准单价
                            trade_price: Double, // 进价
                            category_id: Int, // 类别ID

                            // 时间维度
                            date_key: String, // 代理key 20200101
                            date_value: String, // 2020-01-01
                            day_in_year: Int, // day在当年第几天
                            day_in_month: Int, // 当月第几天
                            is_first_day_in_month: String, // 是否当月第一天 y or n
                            is_last_day_in_month: String, // 是否月的最后一天
                            weekday: Int, // 星期几 数字
                            week_in_month: Int, // 月的第几个星期
                            is_first_day_in_week: String, // 是否周一 y or n
                            is_dayoff: String, // 是否休息日 y or n
                            is_workday: String, // 是否工作日 y or n
                            is_holiday: String, // 是否国家法定节假日
                            date_type: String, // 日期类型：workday、weekend、holiday
                            month_number: Int, // 月份数字
                            year: Long, // 年数字
                            quarter_name: String, // 季度名称
                            quarter_number: Int, // 季度数字
                            year_quarter: String, // 年-季度  2020-q1
                            year_month_number: String // 年-月份 2020-01
                          )
