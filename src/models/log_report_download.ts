import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

interface LogReportDownloadInterface extends Model {
    id: number;
    package_id: number;
    client_unique_id: string;
    created_at: Date;
}

export const LogReportDownload = sequelize.define<LogReportDownloadInterface>(
    'LogReportDownload',
    {
        id: {
            type: DataTypes.BIGINT({ length: 20 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        package_id: DataTypes.INTEGER({ length: 10 }),
        client_unique_id: DataTypes.STRING,
        created_at: DataTypes.DATE,
    },
    {
        tableName: 'log_report_download',
        underscored: true,
        updatedAt: false,
        paranoid: true,
    },
);
