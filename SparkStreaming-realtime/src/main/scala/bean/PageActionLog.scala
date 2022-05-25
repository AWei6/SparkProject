package bean

case class PageActionLog(
                          mid: String,
                          user_id: String,
                          province_id: String,
                          channel: String,
                          is_new: String,
                          model: String,
                          operate_system: String,
                          version_code: String,
                          brand: String,
                          page_id: String,
                          last_page_id: String,
                          page_item: String,
                          page_item_type: String,
                          sourceType: String,
                          during_time: Long,
                          actionId: String,
                          actionItem: String,
                          actionItemType: String,
                          actionTs: Long,
                          ts: Long
                        ){}
